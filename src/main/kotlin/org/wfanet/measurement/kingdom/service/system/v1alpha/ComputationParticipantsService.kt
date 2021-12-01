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
import org.wfanet.measurement.api.v2alpha.DuchyCertificateKey
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.DuchyIdentity
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.duchyIdentityFromContext
import org.wfanet.measurement.internal.kingdom.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineStub as InternalComputationParticipantsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ConfirmComputationParticipantRequest as InternalConfirmComputationParticipantRequest
import org.wfanet.measurement.internal.kingdom.FailComputationParticipantRequest as InternalFailComputationParticipantRequest
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntry
import org.wfanet.measurement.internal.kingdom.SetParticipantRequisitionParamsRequest as InternalSetParticipantRequisitionParamsRequest
import org.wfanet.measurement.system.v1alpha.ComputationParticipant
import org.wfanet.measurement.system.v1alpha.ComputationParticipant.RequisitionParams.ProtocolCase
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKey
import org.wfanet.measurement.system.v1alpha.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.ConfirmComputationParticipantRequest
import org.wfanet.measurement.system.v1alpha.FailComputationParticipantRequest
import org.wfanet.measurement.system.v1alpha.SetParticipantRequisitionParamsRequest

class ComputationParticipantsService(
  private val internalComputationParticipantsClient: InternalComputationParticipantsCoroutineStub,
  private val duchyIdentityProvider: () -> DuchyIdentity = ::duchyIdentityFromContext
) : ComputationParticipantsCoroutineImplBase() {
  override suspend fun setParticipantRequisitionParams(
    request: SetParticipantRequisitionParamsRequest
  ): ComputationParticipant {
    return internalComputationParticipantsClient
      .setParticipantRequisitionParams(request.toInternalRequest())
      .toSystemComputationParticipant()
  }

  override suspend fun confirmComputationParticipant(
    request: ConfirmComputationParticipantRequest
  ): ComputationParticipant {
    return internalComputationParticipantsClient
      .confirmComputationParticipant(request.toInternalRequest())
      .toSystemComputationParticipant()
  }

  override suspend fun failComputationParticipant(
    request: FailComputationParticipantRequest
  ): ComputationParticipant {
    return internalComputationParticipantsClient
      .failComputationParticipant(request.toInternalRequest())
      .toSystemComputationParticipant()
  }

  private fun SetParticipantRequisitionParamsRequest.toInternalRequest():
    InternalSetParticipantRequisitionParamsRequest {
    val computationParticipantKey = getAndVerifyComputationParticipantKey(name)
    val duchyCertificateKey =
      grpcRequireNotNull(DuchyCertificateKey.fromName(requisitionParams.duchyCertificate)) {
        "Resource name unspecified or invalid."
      }
    grpcRequire(computationParticipantKey.duchyId == duchyCertificateKey.duchyId) {
      "The owners of the computation_participant and certificate don't match."
    }
    return InternalSetParticipantRequisitionParamsRequest.newBuilder()
      .apply {
        externalComputationId = apiIdToExternalId(computationParticipantKey.computationId)
        externalDuchyId = computationParticipantKey.duchyId
        externalDuchyCertificateId = apiIdToExternalId(duchyCertificateKey.certificateId)
        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
        when (requisitionParams.protocolCase) {
          ProtocolCase.LIQUID_LEGIONS_V2 -> {
            val llv2 = requisitionParams.liquidLegionsV2
            liquidLegionsV2Builder.apply {
              elGamalPublicKey = llv2.elGamalPublicKey
              elGamalPublicKeySignature = llv2.elGamalPublicKeySignature
            }
          }
          ProtocolCase.PROTOCOL_NOT_SET ->
            failGrpc { "protocol not set in the requisition_params." }
        }
      }
      .build()
  }

  private fun ConfirmComputationParticipantRequest.toInternalRequest():
    InternalConfirmComputationParticipantRequest {
    val computationParticipantKey = getAndVerifyComputationParticipantKey(name)
    return InternalConfirmComputationParticipantRequest.newBuilder()
      .apply {
        externalComputationId = apiIdToExternalId(computationParticipantKey.computationId)
        externalDuchyId = computationParticipantKey.duchyId
      }
      .build()
  }

  private fun FailComputationParticipantRequest.toInternalRequest():
    InternalFailComputationParticipantRequest {
    val computationParticipantKey = getAndVerifyComputationParticipantKey(name)
    return InternalFailComputationParticipantRequest.newBuilder()
      .apply {
        externalComputationId = apiIdToExternalId(computationParticipantKey.computationId)
        externalDuchyId = computationParticipantKey.duchyId
        errorMessage = failure.errorMessage
        duchyChildReferenceId = failure.participantChildReferenceId
        if (failure.hasStageAttempt()) {
          stageAttempt = failure.stageAttempt.toInternalStageAttempt()
          errorDetailsBuilder.apply {
            type = MeasurementLogEntry.ErrorDetails.Type.PERMANENT
            errorTime = failure.errorTime
          }
        }
      }
      .build()
  }

  private fun getAndVerifyComputationParticipantKey(name: String): ComputationParticipantKey {
    val computationParticipantKey =
      grpcRequireNotNull(ComputationParticipantKey.fromName(name)) {
        "Resource name unspecified or invalid."
      }
    if (computationParticipantKey.duchyId != duchyIdentityProvider().id) {
      failGrpc(Status.PERMISSION_DENIED) { "The caller doesn't own this ComputationParticipant." }
    }
    return computationParticipantKey
  }
}
