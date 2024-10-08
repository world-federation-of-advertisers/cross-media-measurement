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

import com.google.cloud.spanner.Value
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.toInt64
import org.wfanet.measurement.internal.kingdom.ComputationParticipant
import org.wfanet.measurement.internal.kingdom.FailComputationParticipantRequest
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementFailure
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.duchyMeasurementLogEntry
import org.wfanet.measurement.internal.kingdom.duchyMeasurementLogEntryDetails
import org.wfanet.measurement.internal.kingdom.duchyMeasurementLogEntryStageAttempt
import org.wfanet.measurement.internal.kingdom.measurementFailure
import org.wfanet.measurement.internal.kingdom.measurementLogEntryDetails
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ComputationParticipantETagMismatchException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ComputationParticipantNotFoundByComputationException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuchyNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ETags
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementStateIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ComputationParticipantReader

private val NEXT_COMPUTATION_PARTICIPANT_STATE = ComputationParticipant.State.FAILED

/**
 * Sets participant details for a ComputationParticipant in the database.
 *
 * Throws the following subclass of [KingdomInternalException] on [execute]:
 * * [ComputationParticipantNotFoundByComputationException] ComputationParticipant not found
 * * [ComputationParticipantETagMismatchException] ComputationParticipant etag mismatch
 * * [DuchyNotFoundException] Duchy not found
 * * [MeasurementStateIllegalException] Measurement is not in state of pending
 */
class FailComputationParticipant(private val request: FailComputationParticipantRequest) :
  SpannerWriter<ComputationParticipant, ComputationParticipant>() {

  override suspend fun TransactionScope.runTransaction(): ComputationParticipant {

    val duchyId =
      DuchyIds.getInternalId(request.externalDuchyId)
        ?: throw DuchyNotFoundException(request.externalDuchyId)

    val computationParticipantResult: ComputationParticipantReader.Result =
      ComputationParticipantReader()
        .readByExternalComputationId(
          transactionContext,
          ExternalId(request.externalComputationId),
          InternalId(duchyId),
        )
        ?: throw ComputationParticipantNotFoundByComputationException(
          ExternalId(request.externalComputationId),
          request.externalDuchyId,
        ) {
          "ComputationParticipant for external computation ID ${request.externalComputationId} " +
            "and external duchy ID ${request.externalDuchyId} not found"
        }

    val (
      computationParticipant,
      measurementId,
      measurementConsumerId,
      measurementState,
      measurementDetails) =
      computationParticipantResult

    if (request.etag.isNotEmpty() && request.etag != computationParticipant.etag) {
      throw ComputationParticipantETagMismatchException(request.etag, computationParticipant.etag)
    }

    when (measurementState) {
      Measurement.State.PENDING_REQUISITION_PARAMS,
      Measurement.State.PENDING_REQUISITION_FULFILLMENT ->
        withdrawRequisitions(measurementConsumerId, measurementId)
      Measurement.State.PENDING_PARTICIPANT_CONFIRMATION,
      Measurement.State.PENDING_COMPUTATION -> {}
      Measurement.State.FAILED,
      Measurement.State.SUCCEEDED,
      Measurement.State.CANCELLED,
      Measurement.State.STATE_UNSPECIFIED,
      Measurement.State.UNRECOGNIZED -> {
        throw MeasurementStateIllegalException(
          ExternalId(computationParticipant.externalMeasurementConsumerId),
          ExternalId(computationParticipant.externalMeasurementId),
          measurementState,
        ) {
          "Unexpected Measurement state $measurementState (${measurementState.number})"
        }
      }
    }

    transactionContext.bufferUpdateMutation("ComputationParticipants") {
      set("MeasurementConsumerId" to measurementConsumerId)
      set("MeasurementId" to measurementId)
      set("DuchyId" to duchyId)
      set("UpdateTime" to Value.COMMIT_TIMESTAMP)
      set("State").toInt64(NEXT_COMPUTATION_PARTICIPANT_STATE)
    }

    val updatedMeasurementDetails =
      measurementDetails.copy {
        failure = measurementFailure {
          reason = MeasurementFailure.Reason.COMPUTATION_PARTICIPANT_FAILED
          message = "Computation Participant failed. ${request.logMessage}"
        }
      }

    val measurementLogEntryDetails = measurementLogEntryDetails {
      logMessage = "Computation Participant failed. ${request.logMessage}"
      this.error = request.error
    }

    val duchyMeasurementLogEntry = duchyMeasurementLogEntry {
      externalDuchyId = request.externalDuchyId
      details = duchyMeasurementLogEntryDetails {
        duchyChildReferenceId = request.duchyChildReferenceId
        stageAttempt = duchyMeasurementLogEntryStageAttempt {
          stage = request.stageAttempt.stage
          stageName = request.stageAttempt.stageName
          stageStartTime = request.stageAttempt.stageStartTime
          attemptNumber = request.stageAttempt.attemptNumber
        }
      }
    }

    updateMeasurementState(
      measurementConsumerId = measurementConsumerId,
      measurementId = measurementId,
      nextState = Measurement.State.FAILED,
      previousState = measurementState,
      measurementLogEntryDetails = measurementLogEntryDetails,
      details = updatedMeasurementDetails,
    )

    insertDuchyMeasurementLogEntry(
      measurementId,
      measurementConsumerId,
      InternalId(duchyId),
      duchyMeasurementLogEntry.details,
    )

    return computationParticipant.copy { state = NEXT_COMPUTATION_PARTICIPANT_STATE }
  }

  override fun ResultScope<ComputationParticipant>.buildResult(): ComputationParticipant {
    return checkNotNull(transactionResult).copy {
      updateTime = commitTimestamp.toProto()
      etag = ETags.computeETag(commitTimestamp)
    }
  }
}
