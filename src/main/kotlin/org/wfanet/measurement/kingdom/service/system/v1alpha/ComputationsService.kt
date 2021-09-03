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

import com.google.protobuf.Timestamp
import java.time.Duration
import java.time.Instant
import java.util.logging.Logger
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.DuchyIdentity
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.duchyIdentityFromContext
import org.wfanet.measurement.common.renewedFlow
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.GetMeasurementByComputationIdRequest
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.internal.kingdom.SetMeasurementResultRequest
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequest
import org.wfanet.measurement.system.v1alpha.Computation
import org.wfanet.measurement.system.v1alpha.ComputationKey
import org.wfanet.measurement.system.v1alpha.ComputationsGrpcKt.ComputationsCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.GetComputationRequest
import org.wfanet.measurement.system.v1alpha.SetComputationResultRequest
import org.wfanet.measurement.system.v1alpha.StreamActiveComputationsRequest
import org.wfanet.measurement.system.v1alpha.StreamActiveComputationsResponse

class ComputationsService(
  private val measurementsClient: MeasurementsCoroutineStub,
  private val duchyIdentityProvider: () -> DuchyIdentity = ::duchyIdentityFromContext,
  private val reconnectInterval: Duration = Duration.ofHours(1),
  private val reconnectDelay: Duration = Duration.ofSeconds(1)
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
    return measurementsClient.getMeasurementByComputationId(internalRequest).toSystemComputation()
  }

  override fun streamActiveComputations(
    request: StreamActiveComputationsRequest
  ): Flow<StreamActiveComputationsResponse> {
    var lastUpdateTime = ContinuationTokenConverter.decode(request.continuationToken)
    return renewedFlow(reconnectInterval, reconnectDelay) {
      logger.info("Streaming active global computations since $lastUpdateTime")
      streamMeasurements(lastUpdateTime)
        .onEach { lastUpdateTime = maxOf(lastUpdateTime, it.updateTime.toInstant()) }
        .map { measurement ->
          StreamActiveComputationsResponse.newBuilder()
            .apply {
              continuationToken =
                ContinuationTokenConverter.encode(measurement.updateTime.toInstant())
              computation = measurement.toSystemComputation()
            }
            .build()
        }
    }
  }

  override suspend fun setComputationResult(request: SetComputationResultRequest): Computation {
    val computationKey =
      grpcRequireNotNull(ComputationKey.fromName(request.name)) {
        "Resource name unspecified or invalid."
      }
    val internalRequest =
      SetMeasurementResultRequest.newBuilder()
        .apply {
          externalComputationId = apiIdToExternalId(computationKey.computationId)
          aggregatorCertificate = request.aggregatorCertificate
          resultPublicKey = request.resultPublicKey
          encryptedResult = request.encryptedResult
        }
        .build()
    return measurementsClient.setMeasurementResult(internalRequest).toSystemComputation()
  }

  private fun streamMeasurements(lastUpdateTime: Instant): Flow<Measurement> {
    val request =
      StreamMeasurementsRequest.newBuilder()
        .apply {
          filterBuilder.apply {
            updatedAfter = lastUpdateTime.toProtoTime()
            addAllStates(STATES_SUBSCRIBED)
          }
        }
        .build()
    return measurementsClient.streamMeasurements(request)
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
    Measurement.State.CANCELLED
  )

private object ContinuationTokenConverter {
  fun encode(time: Instant): String = time.toProtoTime().toByteArray().base64UrlEncode()
  fun decode(token: String): Instant = Timestamp.parseFrom(token.base64UrlDecode()).toInstant()
}
