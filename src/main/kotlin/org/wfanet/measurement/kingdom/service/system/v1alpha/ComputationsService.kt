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
import java.time.Duration
import java.util.logging.Logger
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import org.wfanet.measurement.api.v2alpha.DuchyCertificateKey
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.DuchyIdentity
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.duchyIdentityFromContext
import org.wfanet.measurement.common.renewedFlow
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
    var currentContinuationToken = ContinuationTokenConverter.decode(request.continuationToken)
    return renewedFlow(reconnectInterval, reconnectDelay) {
      logger.info("Streaming active global computations since $currentContinuationToken")
      streamMeasurements(currentContinuationToken)
        .onEach {
          currentContinuationToken = streamActiveComputationsContinuationToken {
            updateTimeSince = it.updateTime
            lastSeenExternalComputationId = it.externalComputationId
          }
        }
        .map { measurement ->
          StreamActiveComputationsResponse.newBuilder()
            .apply {
              continuationToken = ContinuationTokenConverter.encode(currentContinuationToken)
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
    return measurementsClient.setMeasurementResult(internalRequest).toSystemComputation()
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
  fun encode(token: StreamActiveComputationsContinuationToken): String =
    token.toByteArray().base64UrlEncode()
  fun decode(token: String): StreamActiveComputationsContinuationToken =
    StreamActiveComputationsContinuationToken.parseFrom(token.base64UrlDecode())
}
