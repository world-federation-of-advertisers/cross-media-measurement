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
import java.util.concurrent.ConcurrentHashMap
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
import kotlinx.coroutines.isActive
import org.wfanet.measurement.api.v2alpha.DuchyCertificateKey
import org.wfanet.measurement.api.v2alpha.Measurement.Result as MeasurementResult
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.DuchyIdentity
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.duchyIdentityFromContext
import org.wfanet.measurement.common.telemetry.ReportTraceAttributes
import org.wfanet.measurement.common.telemetry.ReportTracing
import org.wfanet.measurement.internal.kingdom.GetMeasurementByComputationIdRequest
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.internal.kingdom.Population
import org.wfanet.measurement.internal.kingdom.PopulationKey
import org.wfanet.measurement.internal.kingdom.PopulationsGrpcKt.PopulationsCoroutineStub
import org.wfanet.measurement.internal.kingdom.SetMeasurementResultRequest
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequestKt
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.computationKey
import org.wfanet.measurement.internal.kingdom.getPopulationRequest
import org.wfanet.measurement.internal.kingdom.setMeasurementResultRequest
import org.wfanet.measurement.internal.kingdom.streamMeasurementsRequest
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
 * @param populationsClient used to read the `PopulationSpec` of a TrusTEE v2 multimeasurement's
 *   pinned `Population`
 */
class ComputationsService(
  private val measurementsClient: MeasurementsCoroutineStub,
  private val populationsClient: PopulationsCoroutineStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
  private val duchyIdentityProvider: () -> DuchyIdentity = ::duchyIdentityFromContext,
  private val streamingTimeout: Duration = 10.minutes,
  private val streamingThrottle: Duration = 1.seconds,
  private val streamingLimit: Int = DEFAULT_STREAMING_LIMIT,
) : ComputationsCoroutineImplBase(coroutineContext) {
  /** System `PopulationSpec` of each `Population` read so far. */
  private val populationSpecs: MutableMap<PopulationKey, PopulationSpec> = ConcurrentHashMap()

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
        throw e.toSystemStatusRuntimeException()
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
            throw cause.toSystemStatusRuntimeException()
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

      grpcRequire(request.hasEncryptedResult() || request.hasResult()) {
        "result_payload unspecified"
      }
      val authenticatedDuchy: DuchyIdentity = duchyIdentityProvider()
      val externalComputationId = apiIdToExternalId(computationKey.computationId)

      // The internal service rejects a payload that does not match the Measurement's protocol.
      val internalRequest =
        if (request.hasResult()) {
          buildPlaintextResultRequest(request, externalComputationId, authenticatedDuchy)
        } else {
          buildEncryptedResultRequest(request, externalComputationId, authenticatedDuchy)
        }
      val measurement: Measurement =
        try {
          measurementsClient.setMeasurementResult(internalRequest)
        } catch (e: StatusException) {
          throw e.toSystemStatusRuntimeException(
            Status.Code.INVALID_ARGUMENT to
              Status.INVALID_ARGUMENT.withDescription(e.status.description),
            Status.Code.FAILED_PRECONDITION to
              Status.FAILED_PRECONDITION.withDescription(e.status.description),
          )
        }
      val computation: Computation = measurement.toResolvedSystemComputation()
      Span.current()
        .setAttribute(ReportTraceAttributes.MEASUREMENT_NAME, computation.measurement)
        .setAttribute(ReportTraceAttributes.OUTCOME, "accepted")
      computation
    }

  /**
   * Builds the internal request for the plaintext multimeasurement `Result` of a TrusTEE v2
   * computation, which is neither signed nor encrypted and so has no aggregator certificate.
   */
  private fun buildPlaintextResultRequest(
    request: SetComputationResultRequest,
    externalComputationId: Long,
    authenticatedDuchy: DuchyIdentity,
  ): SetMeasurementResultRequest {
    val parsedResult: MeasurementResult =
      try {
        MeasurementResult.parseFrom(request.result)
      } catch (e: InvalidProtocolBufferException) {
        throw Status.INVALID_ARGUMENT.withCause(e)
          .withDescription("result is not a serialized Result")
          .asRuntimeException()
      }
    grpcRequire(parsedResult.hasMultimeasurementResult()) {
      "result must have a multimeasurement_result for a TrusTEE v2 Computation"
    }

    return setMeasurementResultRequest {
      this.externalComputationId = externalComputationId
      externalAggregatorDuchyId = authenticatedDuchy.id
      this.result = request.result
      publicApiVersion = request.publicApiVersion
    }
  }

  /**
   * Builds the internal request for the encrypted and signed `Result` of a computation, which is
   * verified using the aggregator certificate.
   */
  private fun buildEncryptedResultRequest(
    request: SetComputationResultRequest,
    externalComputationId: Long,
    authenticatedDuchy: DuchyIdentity,
  ): SetMeasurementResultRequest {
    // This assumes that the Certificate resource name is compatible with public API version
    // v2alpha.
    val aggregatorCertificateKey =
      grpcRequireNotNull(DuchyCertificateKey.fromName(request.aggregatorCertificate)) {
        "aggregator_certificate unspecified or invalid"
      }
    if (aggregatorCertificateKey.duchyId != authenticatedDuchy.id) {
      throw Status.PERMISSION_DENIED.withDescription(
          "Aggregator certificate not owned by authenticated Duchy"
        )
        .asRuntimeException()
    }

    return setMeasurementResultRequest {
      this.externalComputationId = externalComputationId
      resultPublicKey = request.resultPublicKey
      externalAggregatorDuchyId = aggregatorCertificateKey.duchyId
      externalAggregatorCertificateId = apiIdToExternalId(aggregatorCertificateKey.certificateId)
      encryptedResult = request.encryptedResult
      publicApiVersion = request.publicApiVersion
    }
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

  /** Returns the `PopulationSpec` of the `Population` pinned when [measurement] was created. */
  private suspend fun getPopulationSpec(measurement: Measurement): PopulationSpec {
    if (!measurement.details.hasPopulation()) {
      throw Status.FAILED_PRECONDITION.withDescription("Multimeasurement has no Population")
        .asRuntimeException()
    }
    val key: PopulationKey = measurement.details.population
    // A Population is immutable, so each is read once.
    populationSpecs[key]?.let {
      return it
    }
    val population: Population =
      try {
        populationsClient.getPopulation(
          getPopulationRequest {
            externalDataProviderId = key.externalDataProviderId
            externalPopulationId = key.externalPopulationId
          }
        )
      } catch (e: StatusException) {
        throw e.toSystemStatusRuntimeException(
          Status.Code.NOT_FOUND to
            Status.FAILED_PRECONDITION.withDescription("Population not found")
        )
      }
    if (!population.details.hasPopulationSpec()) {
      throw Status.FAILED_PRECONDITION.withDescription("Population has no PopulationSpec")
        .asRuntimeException()
    }
    val populationSpec: PopulationSpec = population.details.populationSpec.toSystemPopulationSpec()
    populationSpecs[key] = populationSpec
    return populationSpec
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
      throw e.toSystemStatusRuntimeException()
    }
  }

  companion object {
    /**
     * Default limit for [org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequest]
     * messages.
     */
    const val DEFAULT_STREAMING_LIMIT = 50

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

/**
 * Converts this [StatusException] from an internal service to one for the system API.
 *
 * Deadline and cancellation codes pass through, a code in [overrides] becomes its [Status], and any
 * other becomes `UNKNOWN`, so that internal details do not leak.
 */
private fun StatusException.toSystemStatusRuntimeException(
  vararg overrides: Pair<Status.Code, Status>
): StatusRuntimeException {
  val code: Status.Code = status.code
  val systemStatus: Status =
    overrides.toMap()[code]
      ?: when (code) {
        Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
        Status.Code.CANCELLED -> Status.CANCELLED
        else -> Status.UNKNOWN
      }
  return systemStatus.withCause(this).asRuntimeException()
}

private object ContinuationTokenConverter {
  fun encode(token: StreamActiveComputationsContinuationToken): String =
    token.toByteArray().base64UrlEncode()

  fun decode(token: String): StreamActiveComputationsContinuationToken =
    StreamActiveComputationsContinuationToken.parseFrom(token.base64UrlDecode())
}
