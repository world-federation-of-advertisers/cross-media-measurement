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
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.api.v2alpha.DuchyCertificateKey
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.ApiId
import org.wfanet.measurement.common.identity.DuchyIdentity
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.duchyIdentityFromContext
import org.wfanet.measurement.internal.kingdom.ComputationParticipant as InternalComputationParticipant
import org.wfanet.measurement.internal.kingdom.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineStub as InternalComputationParticipantsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ConfirmComputationParticipantRequest as InternalConfirmComputationParticipantRequest
import org.wfanet.measurement.internal.kingdom.FailComputationParticipantRequest as InternalFailComputationParticipantRequest
import org.wfanet.measurement.internal.kingdom.HonestMajorityShareShuffleParams
import org.wfanet.measurement.internal.kingdom.LiquidLegionsV2Params
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntryError as InternalMeasurementLogEntryError
import org.wfanet.measurement.internal.kingdom.SetParticipantRequisitionParamsRequest as InternalSetParticipantRequisitionParamsRequest
import org.wfanet.measurement.internal.kingdom.TrusTeeParams
import org.wfanet.measurement.internal.kingdom.confirmComputationParticipantRequest as internalConfirmComputationParticipantRequest
import org.wfanet.measurement.internal.kingdom.failComputationParticipantRequest as internalFailComputationParticipantRequest
import org.wfanet.measurement.internal.kingdom.getComputationParticipantRequest as internalGetComputationParticipantRequest
import org.wfanet.measurement.internal.kingdom.honestMajorityShareShuffleParams
import org.wfanet.measurement.internal.kingdom.liquidLegionsV2Params
import org.wfanet.measurement.internal.kingdom.measurementLogEntryError as internalMeasurementLogEntryError
import org.wfanet.measurement.internal.kingdom.setParticipantRequisitionParamsRequest as internalSetParticipantRequisitionParamsRequest
import org.wfanet.measurement.system.v1alpha.ComputationParticipant
import org.wfanet.measurement.system.v1alpha.ComputationParticipant.RequisitionParams.ProtocolCase
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKey
import org.wfanet.measurement.system.v1alpha.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.ConfirmComputationParticipantRequest
import org.wfanet.measurement.system.v1alpha.FailComputationParticipantRequest
import org.wfanet.measurement.system.v1alpha.GetComputationParticipantRequest
import org.wfanet.measurement.system.v1alpha.SetParticipantRequisitionParamsRequest

class ComputationParticipantsService(
  private val internalComputationParticipantsClient: InternalComputationParticipantsCoroutineStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
  private val duchyIdentityProvider: () -> DuchyIdentity = ::duchyIdentityFromContext,
) : ComputationParticipantsCoroutineImplBase(coroutineContext) {
  override suspend fun getComputationParticipant(
    request: GetComputationParticipantRequest
  ): ComputationParticipant {
    val participantKey: ComputationParticipantKey =
      getAndVerifyComputationParticipantKey(request.name)
    val internalRequest = internalGetComputationParticipantRequest {
      externalComputationId = ApiId(participantKey.computationId).externalId.value
      externalDuchyId = participantKey.duchyId
    }
    val response: InternalComputationParticipant =
      try {
        internalComputationParticipantsClient.getComputationParticipant(internalRequest)
      } catch (e: StatusException) {
        throw mapStatusException(e).asRuntimeException()
      }

    return response.toSystemComputationParticipant()
  }

  override suspend fun setParticipantRequisitionParams(
    request: SetParticipantRequisitionParamsRequest
  ): ComputationParticipant {
    val internalResponse =
      try {
        internalComputationParticipantsClient.setParticipantRequisitionParams(
          request.toInternalRequest()
        )
      } catch (e: StatusException) {
        throw mapStatusException(e).asRuntimeException()
      }

    return internalResponse.toSystemComputationParticipant()
  }

  override suspend fun confirmComputationParticipant(
    request: ConfirmComputationParticipantRequest
  ): ComputationParticipant {
    val internalResponse =
      try {
        internalComputationParticipantsClient.confirmComputationParticipant(
          request.toInternalRequest()
        )
      } catch (e: StatusException) {
        throw mapStatusException(e).asRuntimeException()
      }

    return internalResponse.toSystemComputationParticipant()
  }

  override suspend fun failComputationParticipant(
    request: FailComputationParticipantRequest
  ): ComputationParticipant {
    val internalResponse =
      try {
        internalComputationParticipantsClient.failComputationParticipant(
          request.toInternalRequest()
        )
      } catch (e: StatusException) {
        throw mapStatusException(e).asRuntimeException()
      }

    return internalResponse.toSystemComputationParticipant()
  }

  /**
   * Naively maps [e] to a [Status].
   *
   * Ideally this should be done on a case-by-base basis.
   */
  private fun mapStatusException(e: StatusException): Status {
    return when (e.status.code) {
      Status.Code.NOT_FOUND -> Status.NOT_FOUND
      Status.Code.FAILED_PRECONDITION -> Status.FAILED_PRECONDITION
      Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
      Status.Code.ABORTED -> Status.ABORTED
      Status.Code.INTERNAL -> Status.INTERNAL
      else -> Status.UNKNOWN
    }.withCause(e)
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

    val source = this
    return internalSetParticipantRequisitionParamsRequest {
      externalComputationId = apiIdToExternalId(computationParticipantKey.computationId)
      externalDuchyId = computationParticipantKey.duchyId
      externalDuchyCertificateId = apiIdToExternalId(duchyCertificateKey.certificateId)
      etag = source.etag

      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      when (requisitionParams.protocolCase) {
        ProtocolCase.LIQUID_LEGIONS_V2 -> {
          liquidLegionsV2 = requisitionParams.liquidLegionsV2.toLlV2Details()
        }
        ProtocolCase.REACH_ONLY_LIQUID_LEGIONS_V2 -> {
          reachOnlyLiquidLegionsV2 = requisitionParams.reachOnlyLiquidLegionsV2.toLlV2Details()
        }
        ProtocolCase.HONEST_MAJORITY_SHARE_SHUFFLE -> {
          honestMajorityShareShuffle = requisitionParams.honestMajorityShareShuffle.toHmssDetails()
        }
        ProtocolCase.TRUS_TEE -> {
          trusTee = TrusTeeParams.getDefaultInstance()
        }
        ProtocolCase.PROTOCOL_NOT_SET -> failGrpc { "protocol not set in the requisition_params." }
      }
    }
  }

  private fun ComputationParticipant.RequisitionParams.LiquidLegionsV2.toLlV2Details():
    LiquidLegionsV2Params {
    val source = this
    return liquidLegionsV2Params {
      elGamalPublicKey = source.elGamalPublicKey
      elGamalPublicKeySignature = source.elGamalPublicKeySignature
      elGamalPublicKeySignatureAlgorithmOid = source.elGamalPublicKeySignatureAlgorithmOid
    }
  }

  private fun ComputationParticipant.RequisitionParams.HonestMajorityShareShuffle.toHmssDetails():
    HonestMajorityShareShuffleParams {
    val source = this
    return honestMajorityShareShuffleParams {
      tinkPublicKey = source.tinkPublicKey
      tinkPublicKeySignature = source.tinkPublicKeySignature
      tinkPublicKeySignatureAlgorithmOid = source.tinkPublicKeySignatureAlgorithmOid
    }
  }

  private fun ConfirmComputationParticipantRequest.toInternalRequest():
    InternalConfirmComputationParticipantRequest {
    val computationParticipantKey = getAndVerifyComputationParticipantKey(name)

    val source = this
    return internalConfirmComputationParticipantRequest {
      externalComputationId = apiIdToExternalId(computationParticipantKey.computationId)
      externalDuchyId = computationParticipantKey.duchyId
      etag = source.etag
    }
  }

  private fun FailComputationParticipantRequest.toInternalRequest():
    InternalFailComputationParticipantRequest {
    val computationParticipantKey = getAndVerifyComputationParticipantKey(name)

    val source = this
    return internalFailComputationParticipantRequest {
      externalComputationId = apiIdToExternalId(computationParticipantKey.computationId)
      externalDuchyId = computationParticipantKey.duchyId
      logMessage = failure.errorMessage
      duchyChildReferenceId = failure.participantChildReferenceId
      if (failure.hasStageAttempt()) {
        stageAttempt = failure.stageAttempt.toInternalStageAttempt()
        error = internalMeasurementLogEntryError {
          type = InternalMeasurementLogEntryError.Type.PERMANENT
          errorTime = failure.errorTime
        }
      }
      etag = source.etag
    }
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
