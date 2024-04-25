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
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Value
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.getProtoMessage
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.setJson
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.kingdom.ComputationParticipant
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntryKt
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.SetParticipantRequisitionParamsRequest
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.CertificateIsInvalidException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ComputationParticipantNotFoundByComputationException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ComputationParticipantStateIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuchyCertificateNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuchyNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementStateIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.CertificateReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ComputationParticipantReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.computationParticipantsInState

/**
 * Sets participant details for a computationParticipant in the database.
 *
 * Throws a subclass of [KingdomInternalException] on [execute].
 *
 * @throws [ComputationParticipantNotFoundByComputationException] ComputationParticipant not found
 * @throws [ComputationParticipantStateIllegalException] ComputationParticipant state is not CREATED
 * @throws [DuchyCertificateNotFoundException] Duchy's Certificate not found
 * @throws [CertificateIsInvalidException] Certificate is invalid
 * @throws [DuchyNotFoundException] Duchy not found
 * @throws [MeasurementStateIllegalException] Measurement state is not PENDING_REQUISITION_PARAMS
 */
class SetParticipantRequisitionParams(private val request: SetParticipantRequisitionParamsRequest) :
  SpannerWriter<ComputationParticipant, ComputationParticipant>() {

  override suspend fun TransactionScope.runTransaction(): ComputationParticipant {
    val externalComputationId = ExternalId(request.externalComputationId)
    val duchyId =
      DuchyIds.getInternalId(request.externalDuchyId)
        ?: throw DuchyNotFoundException(request.externalDuchyId)
    val duchyCertificateId =
      readDuchyCertificateId(InternalId(duchyId), ExternalId(request.externalDuchyCertificateId))

    val certificateResult =
      CertificateReader(CertificateReader.ParentType.DUCHY)
        .fillStatementBuilder {
          appendClause("WHERE DuchyId = @duchyId AND CertificateId = @certificateId")
          bind("duchyId" to duchyId)
          bind("certificateId" to duchyCertificateId)
        }
        .execute(transactionContext)
        .single()

    if (!certificateResult.isValid) {
      throw CertificateIsInvalidException()
    }

    val computationParticipantResult: ComputationParticipantReader.Result =
      ComputationParticipantReader()
        .readByExternalComputationId(transactionContext, externalComputationId, InternalId(duchyId))
        ?: throw ComputationParticipantNotFoundByComputationException(
          externalComputationId,
          request.externalDuchyId,
        ) {
          "ComputationParticipant for external computation ID ${request.externalComputationId} " +
            "and external duchy ID ${request.externalDuchyId} not found"
        }

    val computationParticipant = computationParticipantResult.computationParticipant
    if (
      computationParticipantResult.measurementState != Measurement.State.PENDING_REQUISITION_PARAMS
    ) {
      throw MeasurementStateIllegalException(
        ExternalId(computationParticipant.externalMeasurementConsumerId),
        ExternalId(computationParticipant.externalMeasurementId),
        computationParticipantResult.measurementState,
      )
    }

    val measurementId = InternalId(computationParticipantResult.measurementId)
    val measurementConsumerId = InternalId(computationParticipantResult.measurementConsumerId)

    if (computationParticipant.state != ComputationParticipant.State.CREATED) {
      throw ComputationParticipantStateIllegalException(
        externalComputationId,
        request.externalDuchyId,
        computationParticipant.state,
      ) {
        "ComputationParticipant for external computation Id ${request.externalComputationId} " +
          "and external duchy ID ${request.externalDuchyId} has the wrong state. " +
          "It should have been in state CREATED but was in state ${computationParticipant.state}"
      }
    }

    val participantDetails =
      computationParticipant.details.copy {
        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum fields cannot be null.
        when (request.protocolCase) {
          SetParticipantRequisitionParamsRequest.ProtocolCase.LIQUID_LEGIONS_V2 -> {
            liquidLegionsV2 = request.liquidLegionsV2
          }
          SetParticipantRequisitionParamsRequest.ProtocolCase.REACH_ONLY_LIQUID_LEGIONS_V2 -> {
            reachOnlyLiquidLegionsV2 = request.reachOnlyLiquidLegionsV2
          }
          SetParticipantRequisitionParamsRequest.ProtocolCase.HONEST_MAJORITY_SHARE_SHUFFLE -> {
            honestMajorityShareShuffle = request.honestMajorityShareShuffle
          }
          SetParticipantRequisitionParamsRequest.ProtocolCase.PROTOCOL_NOT_SET -> {
            error("Unspecified protocol case in SetParticipantRequisitionParamsRequest.")
          }
        }
      }

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum fields cannot be null.
    val nextState =
      when (request.protocolCase) {
        SetParticipantRequisitionParamsRequest.ProtocolCase.LIQUID_LEGIONS_V2,
        SetParticipantRequisitionParamsRequest.ProtocolCase.REACH_ONLY_LIQUID_LEGIONS_V2 -> {
          ComputationParticipant.State.REQUISITION_PARAMS_SET
        }
        SetParticipantRequisitionParamsRequest.ProtocolCase.HONEST_MAJORITY_SHARE_SHUFFLE -> {
          ComputationParticipant.State.READY
        }
        SetParticipantRequisitionParamsRequest.ProtocolCase.PROTOCOL_NOT_SET -> {
          error("Unspecified protocol case in SetParticipantRequisitionParamsRequest.")
        }
      }

    transactionContext.bufferUpdateMutation("ComputationParticipants") {
      set("MeasurementConsumerId" to measurementConsumerId)
      set("MeasurementId" to measurementId)
      set("DuchyId" to duchyId)
      set("CertificateId" to duchyCertificateId)
      set("UpdateTime" to Value.COMMIT_TIMESTAMP)
      set("State" to nextState)
      set("ParticipantDetails" to participantDetails)
      setJson("ParticipantDetailsJson" to participantDetails)
    }

    val otherComputationParticipants: List<ComputationParticipantDetails> =
      findComputationParticipants(externalComputationId)
        .filter { it.duchyId.value != duchyId }
        .toList()

    if (
      computationParticipantsInState(
        transactionContext,
        otherComputationParticipants.map { it.duchyId },
        measurementConsumerId,
        measurementId,
        nextState,
      )
    ) {
      val measurementLogEntryDetails =
        MeasurementLogEntryKt.details { logMessage = "Pending requisition fulfillment" }
      updateMeasurementState(
        measurementConsumerId = measurementConsumerId,
        measurementId = measurementId,
        nextState = Measurement.State.PENDING_REQUISITION_FULFILLMENT,
        previousState = computationParticipantResult.measurementState,
        measurementLogEntryDetails = measurementLogEntryDetails,
      )
      transactionContext
        .executeQuery(
          statement("SELECT RequisitionId FROM Requisitions") {
            appendClause(
              "WHERE MeasurementConsumerId = @measurementConsumerId AND " +
                "MeasurementId = @measurementId"
            )
            bind("measurementConsumerId" to measurementConsumerId)
            bind("measurementId" to measurementId)
          }
        )
        .collect {
          val requisitionId = it.getLong("RequisitionId")
          transactionContext.bufferUpdateMutation("Requisitions") {
            set("MeasurementConsumerId" to measurementConsumerId)
            set("MeasurementId" to measurementId)
            set("RequisitionId" to requisitionId)
            set("UpdateTime" to Value.COMMIT_TIMESTAMP)
            set("State" to Requisition.State.UNFULFILLED)
            if (request.hasHonestMajorityShareShuffle()) {
              val fulfillingDuchyId =
                selectFulfillingDuchyId(
                  requisitionId,
                  duchyId,
                  participantDetails,
                  otherComputationParticipants,
                )
              set("FulfillingDuchyId" to fulfillingDuchyId)
            }
          }
        }
    }
    return computationParticipant.copy {
      state = nextState
      details = participantDetails
      duchyCertificate = certificateResult.certificate
    }
  }

  override fun ResultScope<ComputationParticipant>.buildResult(): ComputationParticipant {
    return checkNotNull(transactionResult).copy { updateTime = commitTimestamp.toProto() }
  }

  private suspend fun TransactionScope.readDuchyCertificateId(
    duchyId: InternalId,
    externalCertificateId: ExternalId,
  ): InternalId {
    val column = "CertificateId"
    return transactionContext
      .readRowUsingIndex(
        "DuchyCertificates",
        "DuchyCertificatesByExternalId",
        Key.of(duchyId.value, externalCertificateId.value),
        column,
      )
      ?.let { struct -> InternalId(struct.getLong(column)) }
      ?: throw DuchyCertificateNotFoundException(request.externalDuchyId, externalCertificateId) {
        "Certificate for Duchy ${duchyId.value} with external ID " +
          "$externalCertificateId not found"
      }
  }

  private data class ComputationParticipantDetails(
    val duchyId: InternalId,
    val details: ComputationParticipant.Details,
  )

  private fun TransactionScope.findComputationParticipants(
    externalComputationId: ExternalId
  ): Flow<ComputationParticipantDetails> {
    val sql =
      """
      SELECT
        ComputationParticipants.DuchyId, ComputationParticipants.ParticipantDetails
      FROM ComputationParticipants JOIN Measurements USING (MeasurementConsumerId, MeasurementId)
      WHERE ExternalComputationId = @externalComputationId
      """
        .trimIndent()

    val statement: Statement =
      statement(sql) { bind("externalComputationId" to externalComputationId.value) }

    return transactionContext.executeQuery(statement).map {
      val duchyId = InternalId(it.getLong("DuchyId"))
      val details =
        it.getProtoMessage("ParticipantDetails", ComputationParticipant.Details.parser())
      ComputationParticipantDetails(duchyId, details)
    }
  }

  private fun selectFulfillingDuchyId(
    requisitionId: Long,
    currentDuchyId: Long,
    currentParticipantDetails: ComputationParticipant.Details,
    otherParticipantDetails: List<ComputationParticipantDetails>,
  ): Long {
    // TODO(@renjiez): Set the fulfullingDuchyId during Measurement creation by adding duchy roles
    // into HMSS config.
    val candidateDuchyIds = mutableListOf<Long>()

    require(currentParticipantDetails.hasHonestMajorityShareShuffle()) {
      "ComputationParticipantDetails does not have HonestMajorityShareShuffle."
    }
    if (!currentParticipantDetails.honestMajorityShareShuffle.tinkPublicKey.isEmpty) {
      candidateDuchyIds += currentDuchyId
    }
    for (participant in otherParticipantDetails) {
      require(participant.details.hasHonestMajorityShareShuffle()) {
        "ComputationParticipantDetails does not have HonestMajorityShareShuffle."
      }
      if (!participant.details.honestMajorityShareShuffle.tinkPublicKey.isEmpty) {
        candidateDuchyIds += participant.duchyId.value
      }
    }

    require(candidateDuchyIds.size == 2) {
      "Number of computation participant to fulfill requisition is ${candidateDuchyIds.size}."
    }

    candidateDuchyIds.sort()
    return candidateDuchyIds[(requisitionId % candidateDuchyIds.size).toInt()]
  }
}
