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
import org.wfanet.measurement.internal.kingdom.ComputationParticipant
import org.wfanet.measurement.internal.kingdom.FailComputationParticipantRequest
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementKt
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntryKt
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ComputationParticipantNotFoundByComputationException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuchyNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementStateIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ComputationParticipantReader

private val NEXT_COMPUTATION_PARTICIPANT_STATE = ComputationParticipant.State.FAILED

/**
 * Sets participant details for a ComputationParticipant in the database.
 *
 * Throws a subclass of [KingdomInternalException] on [execute].
 *
 * @throws [ComputationParticipantNotFoundByComputationException] ComputationParticipant not found
 * @throws [DuchyNotFoundException] Duchy not found
 * @throws [MeasurementStateIllegalException] Measurement is not in state of pending
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
          InternalId(duchyId)
        )
        ?: throw ComputationParticipantNotFoundByComputationException(
          ExternalId(request.externalComputationId),
          request.externalDuchyId
        ) {
          "ComputationParticipant for external computation ID ${request.externalComputationId} " +
            "and external duchy ID ${request.externalDuchyId} not found"
        }

    val (
      computationParticipant,
      measurementId,
      measurementConsumerId,
      measurementState,
      measurementDetails,
    ) = computationParticipantResult

    when (measurementState) {
      Measurement.State.PENDING_REQUISITION_PARAMS,
      Measurement.State.PENDING_REQUISITION_FULFILLMENT,
      Measurement.State.PENDING_PARTICIPANT_CONFIRMATION,
      Measurement.State.PENDING_COMPUTATION, -> {}
      Measurement.State.FAILED,
      Measurement.State.SUCCEEDED,
      Measurement.State.CANCELLED,
      Measurement.State.STATE_UNSPECIFIED,
      Measurement.State.UNRECOGNIZED, -> {
        throw MeasurementStateIllegalException(
          ExternalId(computationParticipant.externalMeasurementConsumerId),
          ExternalId(computationParticipant.externalMeasurementId),
          measurementState
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
      set("State" to NEXT_COMPUTATION_PARTICIPANT_STATE)
    }

    val updatedMeasurementDetails =
      measurementDetails.copy {
        failure =
          MeasurementKt.failure {
            reason = Measurement.Failure.Reason.COMPUTATION_PARTICIPANT_FAILED
            message = "Computation Participant failed. ${request.errorMessage}"
          }
      }

    val measurementLogEntryDetails =
      MeasurementLogEntryKt.details {
        logMessage = "Computation Participant failed. ${request.errorMessage}"
        this.error = request.errorDetails
      }

    // TODO(@marcopremier): FailComputationParticipant should insert a single MeasurementLogEntry
    // with two children: a StateTransitionMeasurementLogEntries and a DuchyMeasurementLogEntries
    updateMeasurementState(
      measurementConsumerId = InternalId(measurementConsumerId),
      measurementId = InternalId(measurementId),
      nextState = Measurement.State.FAILED,
      previousState = measurementState,
      logDetails = measurementLogEntryDetails,
      details = updatedMeasurementDetails
    )

    return computationParticipant.copy { state = NEXT_COMPUTATION_PARTICIPANT_STATE }
  }

  override fun ResultScope<ComputationParticipant>.buildResult(): ComputationParticipant {
    return checkNotNull(transactionResult).copy { updateTime = commitTimestamp.toProto() }
  }
}
