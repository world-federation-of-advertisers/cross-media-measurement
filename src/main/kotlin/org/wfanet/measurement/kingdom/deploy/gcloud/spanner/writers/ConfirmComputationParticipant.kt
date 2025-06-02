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

import com.google.cloud.spanner.Options
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Value
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.gcloud.spanner.toInt64
import org.wfanet.measurement.internal.kingdom.ComputationParticipant
import org.wfanet.measurement.internal.kingdom.ConfirmComputationParticipantRequest
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.measurementLogEntryDetails
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ComputationParticipantETagMismatchException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ComputationParticipantNotFoundByComputationException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ComputationParticipantStateIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuchyNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ETags
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementStateIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ComputationParticipantReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.computationParticipantsInState

private val NEXT_COMPUTATION_PARTICIPANT_STATE = ComputationParticipant.State.READY

/**
 * Sets participant details for a computationParticipant in the database.
 *
 * Throws the following subclass of [KingdomInternalException] on [execute]:
 * * [ComputationParticipantETagMismatchException] ComputationParticipant etag mismatch
 * * [ComputationParticipantNotFoundByComputationException] ComputationParticipant not found
 * * [ComputationParticipantStateIllegalException] ComputationParticipant state is not
 *   REQUISITION_PARAMS_SET
 * * [DuchyNotFoundException] Duchy not found
 */
class ConfirmComputationParticipant(private val request: ConfirmComputationParticipantRequest) :
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

    val computationParticipant = computationParticipantResult.computationParticipant
    if (request.etag.isNotEmpty() && request.etag != computationParticipant.etag) {
      throw ComputationParticipantETagMismatchException(request.etag, computationParticipant.etag)
    }
    val measurementId = computationParticipantResult.measurementId
    val measurementConsumerId = computationParticipantResult.measurementConsumerId
    val measurementState = computationParticipantResult.measurementState
    if (measurementState != Measurement.State.PENDING_PARTICIPANT_CONFIRMATION) {
      throw MeasurementStateIllegalException(
        ExternalId(computationParticipant.externalMeasurementConsumerId),
        ExternalId(computationParticipant.externalMeasurementId),
        measurementState,
      ) {
        "Measurement for external computation Id ${request.externalComputationId} " +
          "and external duchy ID ${request.externalDuchyId} has the wrong state. " +
          "It should have been in state ${Measurement.State.PENDING_PARTICIPANT_CONFIRMATION}  " +
          "but was in state $measurementState"
      }
    }
    if (computationParticipant.state != ComputationParticipant.State.REQUISITION_PARAMS_SET) {
      throw ComputationParticipantStateIllegalException(
        ExternalId(request.externalComputationId),
        request.externalDuchyId,
        computationParticipant.state,
      ) {
        "ComputationParticipant for external computation Id ${request.externalComputationId} " +
          "and external duchy ID ${request.externalDuchyId} has the wrong state. " +
          "It should have been in state ${ComputationParticipant.State.REQUISITION_PARAMS_SET} " +
          "but was in state ${computationParticipant.state}"
      }
    }

    transactionContext.bufferUpdateMutation("ComputationParticipants") {
      set("MeasurementConsumerId" to measurementConsumerId)
      set("MeasurementId" to measurementId)
      set("DuchyId" to duchyId)
      set("UpdateTime" to Value.COMMIT_TIMESTAMP)
      set("State").toInt64(NEXT_COMPUTATION_PARTICIPANT_STATE)
    }

    val duchyIds: List<InternalId> =
      getComputationParticipantsDuchyIds(measurementConsumerId, measurementId).filter {
        it.value != duchyId
      }

    if (
      computationParticipantsInState(
        transactionContext,
        duchyIds,
        measurementConsumerId,
        measurementId,
        NEXT_COMPUTATION_PARTICIPANT_STATE,
      )
    ) {

      val measurementLogEntryDetails = measurementLogEntryDetails {
        logMessage = "All participants are in status == READY. Measurement.STATE is now PENDING"
      }

      updateMeasurementState(
        measurementConsumerId = measurementConsumerId,
        measurementId = measurementId,
        nextState = Measurement.State.PENDING_COMPUTATION,
        previousState = measurementState,
        measurementLogEntryDetails = measurementLogEntryDetails,
      )
    }

    return computationParticipant.copy { state = NEXT_COMPUTATION_PARTICIPANT_STATE }
  }

  private suspend fun TransactionScope.getComputationParticipantsDuchyIds(
    measurementConsumerId: InternalId,
    measurementId: InternalId,
  ): List<InternalId> {
    val sql =
      """
      SELECT DuchyId
      FROM ComputationParticipants
      WHERE MeasurementConsumerId = @measurement_consumer_id
        AND MeasurementId = @measurement_id
      """
        .trimIndent()
    val statement: Statement =
      statement(sql) {
        bind("measurement_consumer_id" to measurementConsumerId.value)
        bind("measurement_id" to measurementId)
      }
    return transactionContext
      .executeQuery(
        statement,
        Options.tag("writer=$writerName,action=getComputationParticipantsDuchyIds"),
      )
      .map { InternalId(it.getLong("DuchyId")) }
      .toList()
  }

  override fun ResultScope<ComputationParticipant>.buildResult(): ComputationParticipant {
    return checkNotNull(transactionResult).copy {
      updateTime = commitTimestamp.toProto()
      etag = ETags.computeETag(commitTimestamp)
    }
  }
}
