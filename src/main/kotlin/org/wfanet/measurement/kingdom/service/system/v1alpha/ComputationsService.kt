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

import io.grpc.Status
import io.grpc.StatusException
import java.util.logging.Logger
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime
import kotlin.time.TimeMark
import kotlin.time.TimeSource
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.isActive
import org.wfanet.measurement.api.v2alpha.DuchyCertificateKey
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.DuchyIdentity
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.duchyIdentityFromContext
import org.wfanet.measurement.internal.kingdom.GetMeasurementByComputationIdRequest
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.setMeasurementResultRequest
import org.wfanet.measurement.internal.kingdom.streamMeasurementsRequest
import org.wfanet.measurement.system.v1alpha.Computation
import org.wfanet.measurement.system.v1alpha.ComputationKey
import org.wfanet.measurement.system.v1alpha.ComputationsGrpcKt.ComputationsCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.GetComputationRequest
import org.wfanet.measurement.system.v1alpha.SetComputationResultRequest
import org.wfanet.measurement.system.v1alpha.StreamActiveComputationsContinuationToken
import org.wfanet.measurement.system.v1alpha.StreamActiveComputationsRequest
import org.wfanet.measurement.system.v1alpha.StreamActiveComputationsResponse
import org.wfanet.measurement.system.v1alpha.streamActiveComputationsContinuationToken
import org.wfanet.measurement.system.v1alpha.streamActiveComputationsResponse

@OptIn(ExperimentalTime::class)
class ComputationsService(
  private val measurementsClient: MeasurementsCoroutineStub,
  private val duchyIdentityProvider: () -> DuchyIdentity = ::duchyIdentityFromContext,
  private val streamingTimeout: Duration = 10.minutes,
  private val streamingThrottle: Duration = 1.seconds
) : ComputationsCoroutineImplBase() {
  override suspend fun getComputation(request: GetComputationRequest): Computation {
    val computationKey =
      grpcRequireNotNull(ComputationKey.fromName(request.name)) {
        "Resource name unspecified or invalid."
      }
    val internalRequest =
      GetMeasurementByComputationIdRequest.newBuilder()
        .apply { externalComputationId = apiIdToExternalId(computationKey.computationId) }
        .build()
    try {
      return measurementsClient.getMeasurementByComputationId(internalRequest).toSystemComputation()
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

  override fun streamActiveComputations(
    request: StreamActiveComputationsRequest
  ): Flow<StreamActiveComputationsResponse> {
    val streamingDeadline: TimeMark = TimeSource.Monotonic.markNow() + streamingTimeout
    var currentContinuationToken = ContinuationTokenConverter.decode(request.continuationToken)
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
            currentContinuationToken = streamActiveComputationsContinuationToken {
              updateTimeSince = measurement.updateTime
              lastSeenExternalComputationId = measurement.externalComputationId
            }
            val response = streamActiveComputationsResponse {
              continuationToken = ContinuationTokenConverter.encode(currentContinuationToken)
              computation = measurement.toSystemComputation()
            }
            emit(response)
          }

        delay(streamingThrottle)
      }
    }
  }

  override suspend fun setComputationResult(request: SetComputationResultRequest): Computation {
    val computationKey =
      grpcRequireNotNull(ComputationKey.fromName(request.name)) {
        "Resource name unspecified or invalid."
      }

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
    }
    try {
      return measurementsClient.setMeasurementResult(internalRequest).toSystemComputation()
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

  private fun streamMeasurements(
    continuationToken: StreamActiveComputationsContinuationToken
  ): Flow<Measurement> {
    val request = streamMeasurementsRequest {
      filter = filter {
        updatedAfter = continuationToken.updateTimeSince
        externalComputationIdAfter = continuationToken.lastSeenExternalComputationId
        states += STATES_SUBSCRIBED
        externalDuchyId = duchyIdentityProvider().id
      }
      measurementView = Measurement.View.COMPUTATION
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
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}

private val STATES_SUBSCRIBED =
  listOf(
    Measurement.State.PENDING_REQUISITION_PARAMS,
    Measurement.State.PENDING_PARTICIPANT_CONFIRMATION,
    Measurement.State.PENDING_COMPUTATION,
    Measurement.State.FAILED,
    Measurement.State.CANCELLED,
    Measurement.State.SUCCEEDED
  )

private object ContinuationTokenConverter {
  fun encode(token: StreamActiveComputationsContinuationToken): String =
    token.toByteArray().base64UrlEncode()
  fun decode(token: String): StreamActiveComputationsContinuationToken =
    StreamActiveComputationsContinuationToken.parseFrom(token.base64UrlDecode())
}
