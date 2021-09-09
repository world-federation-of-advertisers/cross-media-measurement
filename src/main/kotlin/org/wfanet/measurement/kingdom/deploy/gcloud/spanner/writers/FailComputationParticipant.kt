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

import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.internal.kingdom.ComputationParticipant
import org.wfanet.measurement.internal.kingdom.FailComputationParticipantRequest
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.computationParticipant
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ComputationParticipantReader

private val NEXT_COMPUTATION_PARTICIPANT_STATE = ComputationParticipant.State.FAILED

/**
 * Sets participant details for a computationPartcipant in the database.
 *
 * Throws a [KingdomInternalException] on [execute] with the following codes/conditions:
 * * [KingdomInternalException.Code.COMPUTATION_PARTICIPANT_NOT_FOUND]
 * * [KingdomInternalException.Code.DUCHY_NOT_FOUND]
 * * [KingdomInternalException.Code.MEASUREMENT_STATE_ILLEGAL]
 */
class FailComputationParticipant(private val request: FailComputationParticipantRequest) :
  SpannerWriter<ComputationParticipant, ComputationParticipant>() {

  override suspend fun TransactionScope.runTransaction(): ComputationParticipant {

    val duchyId =
      DuchyIds.getInternalId(request.externalDuchyId)
        ?: throw KingdomInternalException(KingdomInternalException.Code.DUCHY_NOT_FOUND)

    val computationParticipantResult: ComputationParticipantReader.Result =
      ComputationParticipantReader()
        .readWithIds(
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

    transactionContext.bufferUpdateMutation("ComputationParticipants") {
      set("MeasurementConsumerId" to measurementConsumerId)
      set("MeasurementId" to measurementId)
      set("DuchyId" to duchyId)
      set("State" to NEXT_COMPUTATION_PARTICIPANT_STATE)
    }

    when (val state = computationParticipantResult.measurementState) {
      Measurement.State.PENDING_REQUISITION_PARAMS,
      Measurement.State.PENDING_REQUISITION_FULFILLMENT,
      Measurement.State.PENDING_PARTICIPANT_CONFIRMATION,
      Measurement.State.PENDING_COMPUTATION -> {}
      Measurement.State.SUCCEEDED,
      Measurement.State.FAILED,
      Measurement.State.CANCELLED,
      Measurement.State.STATE_UNSPECIFIED,
      Measurement.State.UNRECOGNIZED -> {
        throw KingdomInternalException(KingdomInternalException.Code.MEASUREMENT_STATE_ILLEGAL) {
          "Unexpected Measurement state $state (${state.number})"
        }
      }
    }

    updateMeasurementState(
      InternalId(measurementConsumerId),
      InternalId(measurementId),
      Measurement.State.FAILED
    )

    return computationParticipant.copy { state = NEXT_COMPUTATION_PARTICIPANT_STATE }
  }

  override fun ResultScope<ComputationParticipant>.buildResult(): ComputationParticipant {
    return checkNotNull(transactionResult).copy { updateTime = commitTimestamp.toProto() }
  }
}
