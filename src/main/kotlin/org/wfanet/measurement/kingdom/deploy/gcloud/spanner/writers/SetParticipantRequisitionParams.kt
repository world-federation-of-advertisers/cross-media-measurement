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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.cloud.spanner.Key
import com.google.cloud.spanner.Value
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.setJson
import org.wfanet.measurement.internal.kingdom.ComputationParticipant
import org.wfanet.measurement.internal.kingdom.ComputationParticipantKt.details
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.SetParticipantRequisitionParamsRequest
import org.wfanet.measurement.internal.kingdom.computationParticipant
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ComputationParticipantReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.computationParticipantsInState

private val NEXT_COMPUTATION_PARTICIPANT_STATE = ComputationParticipant.State.REQUISITION_PARAMS_SET

/**
 * Sets participant details for a computationPartcipant in the database.
 *
 * Throws a [KingdomInternalException] on [execute] with the following codes/conditions:
 * * [KingdomInternalException.Code.COMPUTATION_PARTICIPANT_NOT_FOUND]
 * * [KingdomInternalException.Code.COMPUTATION_PARTICIPANT_STATE_ILLEGAL]
 * * [KingdomInternalException.Code.CERTIFICATE_NOT_FOUND]
 * * [KingdomInternalException.Code.DUCHY_NOT_FOUND]
 */
class SetParticipantRequisitionParams(private val request: SetParticipantRequisitionParamsRequest) :
  SpannerWriter<ComputationParticipant, ComputationParticipant>() {

  override suspend fun TransactionScope.runTransaction(): ComputationParticipant {

    val duchyId =
      DuchyIds.getInternalId(request.externalDuchyId)
        ?: throw KingdomInternalException(KingdomInternalException.Code.DUCHY_NOT_FOUND)
    val duchyCertificateId =
      readDuchyCertificateId(InternalId(duchyId), ExternalId(request.externalDuchyCertificateId))

    val computationParticipantResult: ComputationParticipantReader.Result =
      ComputationParticipantReader()
        .readWithIdsOrNull(
          transactionContext,
          ExternalId(request.externalComputationId),
          InternalId(duchyId)
        )
        ?: throw KingdomInternalException(
          KingdomInternalException.Code.COMPUTATION_PARTICIPANT_NOT_FOUND
        ) {
          "ComputationParticipant for external computation ID ${request.externalComputationId} " +
            "and external duchy ID ${request.externalDuchyId} not found"
        }

    val computationParticipant = computationParticipantResult.computationParticipant
    val measurementId = computationParticipantResult.measurementId
    val measurementConsumerId = computationParticipantResult.measurementConsumerId

    if (computationParticipant.state != ComputationParticipant.State.CREATED) {
      throw KingdomInternalException(
        KingdomInternalException.Code.COMPUTATION_PARTICIPANT_STATE_ILLEGAL
      ) {
        "ComputationParticipant for external computation Id ${request.externalComputationId} " +
          "and external duchy ID ${request.externalDuchyId} has the wrong state. " +
          "It should have been in state CREATED but was in state ${computationParticipant.state}"
      }
    }

    val participantDetails =
      computationParticipant.details.copy { liquidLegionsV2 = request.liquidLegionsV2 }
    transactionContext.bufferUpdateMutation("ComputationParticipants") {
      set("MeasurementConsumerId" to measurementConsumerId)
      set("MeasurementId" to measurementId)
      set("DuchyId" to duchyId)
      set("CertificateId" to duchyCertificateId.value)
      set("UpdateTime" to Value.COMMIT_TIMESTAMP)
      set("State" to NEXT_COMPUTATION_PARTICIPANT_STATE)
      set("ParticipantDetails" to participantDetails)
      setJson("ParticipantDetailsJson" to participantDetails)
    }

    val otherDuchyIds: List<InternalId> =
      DuchyIds.entries.map { InternalId(it.internalDuchyId) }.filter { it.value != duchyId }

    if (computationParticipantsInState(
        transactionContext,
        otherDuchyIds,
        InternalId(measurementConsumerId),
        InternalId(measurementId),
        NEXT_COMPUTATION_PARTICIPANT_STATE
      )
    ) {
      updateMeasurementState(
        InternalId(measurementConsumerId),
        InternalId(measurementId),
        Measurement.State.PENDING_REQUISITION_FULFILLMENT
      )
    }

    return computationParticipant.copy {
      state = NEXT_COMPUTATION_PARTICIPANT_STATE
      externalDuchyCertificateId = request.externalDuchyCertificateId
      details = participantDetails
    }
  }

  override fun ResultScope<ComputationParticipant>.buildResult(): ComputationParticipant {
    return checkNotNull(transactionResult).copy { updateTime = commitTimestamp.toProto() }
  }

  private suspend fun TransactionScope.readDuchyCertificateId(
    duchyId: InternalId,
    externalCertificateId: ExternalId
  ): InternalId {
    val column = "CertificateId"
    return transactionContext.readRowUsingIndex(
        "DuchyCertificates",
        "DuchyCertificatesByExternalId",
        Key.of(duchyId.value, externalCertificateId.value),
        column
      )
      ?.let { struct -> InternalId(struct.getLong(column)) }
      ?: throw KingdomInternalException(KingdomInternalException.Code.CERTIFICATE_NOT_FOUND) {
        "Certificate for Duchy ${duchyId.value} with external ID " +
          "$externalCertificateId not found"
      }
  }
}
