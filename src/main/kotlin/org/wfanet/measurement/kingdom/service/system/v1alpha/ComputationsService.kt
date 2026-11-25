// Copyright 2021 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.kingdom.service.system.v1alpha

import com.google.protobuf.InvalidProtocolBufferException
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.Span
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeMark
import kotlin.time.TimeSource
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.isActive
import org.wfanet.measurement.api.v2alpha.DuchyCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.DuchyIdentity
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.duchyIdentityFromContext
import org.wfanet.measurement.common.telemetry.ReportTraceAttributes
import org.wfanet.measurement.common.telemetry.ReportTracing
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.internal.kingdom.GetMeasurementByComputationIdRequest
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ModelRelease
import org.wfanet.measurement.internal.kingdom.ModelReleasesGrpcKt.ModelReleasesCoroutineStub
import org.wfanet.measurement.internal.kingdom.ModelRollout
import org.wfanet.measurement.internal.kingdom.ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub
import org.wfanet.measurement.internal.kingdom.Population
import org.wfanet.measurement.internal.kingdom.PopulationsGrpcKt.PopulationsCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequestKt
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.StreamModelRolloutsRequestKt
import org.wfanet.measurement.internal.kingdom.computationKey
import org.wfanet.measurement.internal.kingdom.getModelReleaseRequest
import org.wfanet.measurement.internal.kingdom.getPopulationRequest
import org.wfanet.measurement.internal.kingdom.setMeasurementResultRequest
import org.wfanet.measurement.internal.kingdom.streamMeasurementsRequest
import org.wfanet.measurement.internal.kingdom.streamModelRolloutsRequest
import org.wfanet.measurement.system.v1alpha.Computation
import org.wfanet.measurement.system.v1alpha.ComputationKey
import org.wfanet.measurement.system.v1alpha.ComputationsGrpcKt.ComputationsCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.GetComputationRequest
import org.wfanet.measurement.system.v1alpha.PopulationSpec
import org.wfanet.measurement.system.v1alpha.SetComputationResultRequest
import org.wfanet.measurement.system.v1alpha.StreamActiveComputationsContinuationToken
import org.wfanet.measurement.system.v1alpha.StreamActiveComputationsRequest
import org.wfanet.measurement.system.v1alpha.StreamActiveComputationsResponse
import org.wfanet.measurement.system.v1alpha.copy
import org.wfanet.measurement.system.v1alpha.streamActiveComputationsContinuationToken
import org.wfanet.measurement.system.v1alpha.streamActiveComputationsResponse

/**
 * System API `Computations` service.
 *
 * @param modelRolloutsClient used with [modelReleasesClient] and [populationsClient] to resolve
 *   the `PopulationSpec` of a TrusTEE v2 multimeasurement from its model line
 */
class ComputationsService(
  private val measurementsClient: MeasurementsCoroutineStub,
  private val modelRolloutsClient: ModelRolloutsCoroutineStub,
  private val modelReleasesClient: ModelReleasesCoroutineStub,
  private val populationsClient: PopulationsCoroutineStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
  private val duchyIdentityProvider: () -> DuchyIdentity = ::duchyIdentityFromContext,
  private val streamingTimeout: Duration = 10.minutes,
  private val streamingThrottle: Duration = 1.seconds,
  private val streamingLimit: Int = DEFAULT_STREAMING_LIMIT,
) : ComputationsCoroutineImplBase(coroutineContext) {
  override suspend fun getComputation(request: GetComputationRequest): Computation {
    val computationKey =
      grpcRequireNotNull(ComputationKey.fromName(request.name)) {
        "Resource name unspecified or invalid."
      }
    val internalRequest =
      GetMeasurementByComputationIdRequest.newBuilder()
        .apply { externalComputationId = apiIdToExternalId(computationKey.computationId) }
        .build()
    val measurement: Measurement =
      try {
        measurementsClient.getMeasurementByComputationId(internalRequest)
      } catch (e: StatusException) {
        throw when (e.status.code) {
            Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
            Status.Code.CANCELLED -> Status.CANCELLED
            else -> Status.UNKNOWN
          }
          .withCause(e)
          .asRuntimeException()
      }
    return measurement.toResolvedSystemComputation()
  }

  override fun streamActiveComputations(
    request: StreamActiveComputationsRequest
  ): Flow<StreamActiveComputationsResponse> {
    val streamingDeadline: TimeMark = TimeSource.Monotonic.markNow() + streamingTimeout
    var currentContinuationToken: StreamActiveComputationsContinuationToken? =
      if (request.continuationToken.isEmpty()) {
        null
      } else {
        ContinuationTokenConverter.decode(request.continuationToken)
      }
    return flow {
      // Continually request measurements from internal service until cancelled or streamingDeadline
      // is reached.
      //
      // TODO(@SanjayVas): Figure out an alternative mechanism (e.g. Spanner change streams) to
      // avoid having to poll internal service.
      while (currentCoroutineContext().isActive && streamingDeadline.hasNotPassedNow()) {
        streamMeasurements(currentContinuationToken)
          .catch { cause ->
            if (cause !is StatusException) throw cause
            throw when (cause.status.code) {
                Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
                Status.Code.CANCELLED -> Status.CANCELLED
                else -> Status.UNKNOWN
              }
              .withCause(cause)
              .asRuntimeException()
          }
          .collect { measurement ->
            val continuationToken = streamActiveComputationsContinuationToken {
              lastSeenUpdateTime = measurement.updateTime
              lastSeenExternalComputationId = measurement.externalComputationId
            }
            val response = streamActiveComputationsResponse {
              this.continuationToken = ContinuationTokenConverter.encode(continuationToken)
              computation = measurement.toResolvedSystemComputation()
            }
            currentContinuationToken = continuationToken
            emit(response)
          }

        delay(streamingThrottle)
      }
    }
  }

  override suspend fun setComputationResult(request: SetComputationResultRequest): Computation =
    ReportTracing.traceSuspending(
      "kingdom.computation.result_acceptance",
      Attributes.builder()
        .put(ReportTraceAttributes.COMPUTATION_NAME, request.name)
        .put(ReportTraceAttributes.LIFECYCLE_STAGE, "kingdom_computation_result_acceptance")
        .put(ReportTraceAttributes.OUTCOME, "started")
        .build(),
    ) {
      val computationKey =
        grpcRequireNotNull(ComputationKey.fromName(request.name)) {
          "Resource name unspecified or invalid."
        }
      grpcRequire(request.publicApiVersion.isNotEmpty()) { "public_api_version unspecified" }

      // This assumes that the Certificate resource name is compatible with public API version
      // v2alpha.
      val aggregatorCertificateKey =
        grpcRequireNotNull(DuchyCertificateKey.fromName(request.aggregatorCertificate)) {
          "aggregator_certificate unspecified or invalid"
        }
      val authenticatedDuchy: DuchyIdentity = duchyIdentityProvider()
      if (aggregatorCertificateKey.duchyId != authenticatedDuchy.id) {
        throw Status.PERMISSION_DENIED.withDescription(
            "Aggregator certificate not owned by authenticated Duchy"
          )
          .asRuntimeException()
      }

      val internalRequest = setMeasurementResultRequest {
        externalComputationId = apiIdToExternalId(computationKey.computationId)
        resultPublicKey = request.resultPublicKey
        externalAggregatorDuchyId = aggregatorCertificateKey.duchyId
        externalAggregatorCertificateId = apiIdToExternalId(aggregatorCertificateKey.certificateId)
        encryptedResult = request.encryptedResult
        publicApiVersion = request.publicApiVersion
      }
      val measurement: Measurement =
        try {
          measurementsClient.setMeasurementResult(internalRequest)
        } catch (e: StatusException) {
          throw when (e.status.code) {
              Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
              Status.Code.CANCELLED -> Status.CANCELLED
              else -> Status.UNKNOWN
            }
            .withCause(e)
            .asRuntimeException()
        }
      val computation: Computation = measurement.toResolvedSystemComputation()
      Span.current()
        .setAttribute(ReportTraceAttributes.MEASUREMENT_NAME, computation.measurement)
        .setAttribute(ReportTraceAttributes.OUTCOME, "accepted")
      computation
    }

  /**
   * Converts this [Measurement] to a system [Computation], resolving the `population_spec` of a
   * multimeasurement.
   */
  private suspend fun Measurement.toResolvedSystemComputation(): Computation {
    val computation: Computation = toSystemComputation()
    if (!computation.hasMultimeasurement()) {
      return computation
    }
    val resolvedPopulationSpec: PopulationSpec = getPopulationSpec(this)
    // TODO(world-federation-of-advertisers/cross-media-measurement#4532): Set `event_descriptor`
    //  once the Kingdom stores the event message `FileDescriptorSet` for a model line's
    //  event-template version. The Kingdom stores only the version number.
    return computation.copy {
      multimeasurement = multimeasurement.copy { populationSpec = resolvedPopulationSpec }
    }
  }

  /**
   * Returns the `PopulationSpec` of the `Population` of the latest `ModelRollout` of the model line
   * of [measurement].
   */
  private suspend fun getPopulationSpec(measurement: Measurement): PopulationSpec {
    val measurementSpec: MeasurementSpec =
      try {
        MeasurementSpec.parseFrom(measurement.details.measurementSpec)
      } catch (e: InvalidProtocolBufferException) {
        throw Status.INTERNAL.withDescription("MeasurementSpec is invalid")
          .withCause(e)
          .asRuntimeException()
      }
    val modelLineKey: ModelLineKey =
      ModelLineKey.fromName(measurementSpec.modelLine)
        ?: throw Status.FAILED_PRECONDITION.withDescription(
            "MeasurementSpec model_line unspecified or invalid"
          )
          .asRuntimeException()

    val modelRollouts: List<ModelRollout> =
      try {
        modelRolloutsClient
          .streamModelRollouts(
            streamModelRolloutsRequest {
              filter =
                StreamModelRolloutsRequestKt.filter {
                  externalModelProviderId = apiIdToExternalId(modelLineKey.modelProviderId)
                  externalModelSuiteId = apiIdToExternalId(modelLineKey.modelSuiteId)
                  externalModelLineId = apiIdToExternalId(modelLineKey.modelLineId)
                }
            }
          )
          .toList()
      } catch (e: StatusException) {
        throw e.toSystemStatusRuntimeException()
      }
    // Matches how the Population DataProvider resolves the Population for a model line.
    val latestModelRollout: ModelRollout =
      modelRollouts.maxWithOrNull(MODEL_ROLLOUT_RECENCY_COMPARATOR)
        ?: throw Status.FAILED_PRECONDITION.withDescription(
            "ModelLine ${measurementSpec.modelLine} has no ModelRollout"
          )
          .asRuntimeException()

    val modelRelease: ModelRelease =
      try {
        modelReleasesClient.getModelRelease(
          getModelReleaseRequest {
            externalModelProviderId = latestModelRollout.externalModelProviderId
            externalModelSuiteId = latestModelRollout.externalModelSuiteId
            externalModelReleaseId = latestModelRollout.externalModelReleaseId
          }
        )
      } catch (e: StatusException) {
        throw e.toSystemStatusRuntimeException()
      }
    val population: Population =
      try {
        populationsClient.getPopulation(
          getPopulationRequest {
            externalDataProviderId = modelRelease.externalDataProviderId
            externalPopulationId = modelRelease.externalPopulationId
          }
        )
      } catch (e: StatusException) {
        throw e.toSystemStatusRuntimeException()
      }
    if (!population.details.hasPopulationSpec()) {
      throw Status.FAILED_PRECONDITION.withDescription(
          "Population for ModelLine ${measurementSpec.modelLine} has no PopulationSpec"
        )
        .asRuntimeException()
    }
    return population.details.populationSpec.toSystemPopulationSpec()
  }

  private fun streamMeasurements(
    continuationToken: StreamActiveComputationsContinuationToken?
  ): Flow<Measurement> {
    val request = streamMeasurementsRequest {
      filter = filter {
        if (continuationToken != null) {
          after =
            StreamMeasurementsRequestKt.FilterKt.after {
              updateTime = continuationToken.lastSeenUpdateTime
              computation = computationKey {
                externalComputationId = continuationToken.lastSeenExternalComputationId
              }
            }
        }

        states += STATES_SUBSCRIBED
        externalDuchyId = duchyIdentityProvider().id
      }
      measurementView = Measurement.View.COMPUTATION
      limit = streamingLimit
    }
    try {
      return measurementsClient.streamMeasurements(request)
    } catch (e: StatusException) {
      throw when (e.status.code) {
          Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
          Status.Code.CANCELLED -> Status.CANCELLED
          else -> Status.UNKNOWN
        }
        .withCause(e)
        .asRuntimeException()
    }
  }

  companion object {
    /**
     * Default limit for [org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequest]
     * messages.
     */
    const val DEFAULT_STREAMING_LIMIT = 50

    /** Orders [ModelRollout]s from least to most recent. */
    private val MODEL_ROLLOUT_RECENCY_COMPARATOR: Comparator<ModelRollout> =
      compareBy({ it.rolloutPeriodStartTime.toInstant() }, { it.createTime.toInstant() })

    private val STATES_SUBSCRIBED =
      listOf(
        Measurement.State.PENDING_REQUISITION_PARAMS,
        Measurement.State.PENDING_PARTICIPANT_CONFIRMATION,
        Measurement.State.PENDING_COMPUTATION,
        Measurement.State.FAILED,
        Measurement.State.CANCELLED,
        Measurement.State.SUCCEEDED,
      )
  }
}

/** Converts this [StatusException] from an internal service to one for the system API. */
private fun StatusException.toSystemStatusRuntimeException(): StatusRuntimeException {
  val status: Status =
    when (status.code) {
      Status.Code.NOT_FOUND -> Status.FAILED_PRECONDITION.withDescription(status.description)
      Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
      Status.Code.CANCELLED -> Status.CANCELLED
      else -> Status.UNKNOWN
    }
  return status.withCause(this).asRuntimeException()
}

private object ContinuationTokenConverter {
  fun encode(token: StreamActiveComputationsContinuationToken): String =
    token.toByteArray().base64UrlEncode()

  fun decode(token: String): StreamActiveComputationsContinuationToken =
    StreamActiveComputationsContinuationToken.parseFrom(token.base64UrlDecode())
}
