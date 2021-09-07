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
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.makeStatement
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.internal.kingdom.FulfillRequisitionRequest
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.RequisitionReader

private object Params {
  const val MEASUREMENT_CONSUMER_ID = "measurementConsumerId"
  const val MEASUREMENT_ID = "measurementId"
  const val REQUISITION_STATE = "requisitionState"
}

/**
 * Fulfills a [Requisition].
 *
 * Throws a [KingdomInternalException] on [execute] with the following codes/conditions:
 * * [KingdomInternalException.Code.MEASUREMENT_STATE_ILLEGAL]
 * * [KingdomInternalException.Code.REQUISITION_STATE_ILLEGAL]
 * * [KingdomInternalException.Code.REQUISITION_NOT_FOUND]
 * * [KingdomInternalException.Code.DUCHY_NOT_FOUND]
 */
class FulfillRequisition(private val request: FulfillRequisitionRequest) :
  SpannerWriter<Requisition, Requisition>() {
  override suspend fun TransactionScope.runTransaction(): Requisition {
    val readResult: RequisitionReader.Result = readRequisition()
    val (measurementConsumerId, measurementId, requisitionId, requisition) = readResult

    val state = requisition.state
    if (state != Requisition.State.UNFULFILLED) {
      throw KingdomInternalException(KingdomInternalException.Code.REQUISITION_STATE_ILLEGAL) {
        "Expected ${Requisition.State.UNFULFILLED}, got $state"
      }
    }
    val measurementState = requisition.parentMeasurement.state
    if (measurementState != Measurement.State.PENDING_REQUISITION_FULFILLMENT) {
      throw KingdomInternalException(KingdomInternalException.Code.MEASUREMENT_STATE_ILLEGAL) {
        "Expected ${Measurement.State.PENDING_REQUISITION_FULFILLMENT}, got $state"
      }
    }

    val updatedDetails =
      requisition.details.copy {
        dataProviderParticipationSignature = request.dataProviderParticipationSignature
      }

    val nonFulfilledRequisitionIds =
      readRequisitionsNotInState(measurementConsumerId, measurementId, Requisition.State.FULFILLED)
    val updatedMeasurementState: Measurement.State? =
      if (nonFulfilledRequisitionIds.singleOrNull() == requisitionId) {
        // All other Requisitions are already FULFILLED, so update Measurement state.
        Measurement.State.PENDING_PARTICIPANT_CONFIRMATION.also {
          updateMeasurementState(measurementConsumerId, measurementId, it)
        }
      } else {
        null
      }

    updateRequisition(readResult, getFulfillingDuchyId(), updatedDetails)

    return requisition.copy {
      externalFulfillingDuchyId = request.externalFulfillingDuchyId
      this.state = Requisition.State.FULFILLED
      details = updatedDetails
      if (updatedMeasurementState != null) {
        parentMeasurement = parentMeasurement.copy { this.state = updatedMeasurementState }
      }
    }
  }

  override fun ResultScope<Requisition>.buildResult(): Requisition {
    return checkNotNull(transactionResult).copy { updateTime = commitTimestamp.toProto() }
  }

  private suspend fun TransactionScope.readRequisition(): RequisitionReader.Result {
    val externalComputationId = request.externalComputationId
    val externalRequisitionId = request.externalRequisitionId

    val readResult: RequisitionReader.Result =
      RequisitionReader()
        .readByExternalComputationId(
          transactionContext,
          externalComputationId = externalComputationId,
          externalRequisitionId = externalRequisitionId
        )
        ?: throw KingdomInternalException(KingdomInternalException.Code.REQUISITION_NOT_FOUND) {
          "Requisition with external Computation ID $externalComputationId and external " +
            "Requisition ID $externalRequisitionId not found"
        }
    return readResult
  }

  private fun getFulfillingDuchyId(): InternalId {
    val externalDuchyId: String = request.externalFulfillingDuchyId
    return DuchyIds.getInternalId(externalDuchyId)?.let { InternalId(it) }
      ?: throw KingdomInternalException(KingdomInternalException.Code.DUCHY_NOT_FOUND) {
        "Duchy with external ID $externalDuchyId not found"
      }
  }

  companion object {
    private fun TransactionScope.updateRequisition(
      readResult: RequisitionReader.Result,
      fulfillingDuchyId: InternalId,
      updatedDetails: Requisition.Details
    ) {
      transactionContext.bufferUpdateMutation("Requisitions") {
        set("MeasurementId" to readResult.measurementId.value)
        set("MeasurementConsumerId" to readResult.measurementConsumerId.value)
        set("RequisitionId" to readResult.requisitionId.value)
        set("UpdateTime" to Value.COMMIT_TIMESTAMP)
        set("State" to Requisition.State.FULFILLED)
        set("FulfillingDuchyId" to fulfillingDuchyId.value)
        set("RequisitionDetails" to updatedDetails)
      }
    }

    private fun TransactionScope.readRequisitionsNotInState(
      measurementConsumerId: InternalId,
      measurementId: InternalId,
      state: Requisition.State
    ): Flow<InternalId> {
      val sql =
        """
        SELECT RequisitionId
        FROM Requisitions
        WHERE
          MeasurementConsumerId = @${Params.MEASUREMENT_CONSUMER_ID}
          AND MeasurementId = @${Params.MEASUREMENT_ID}
          AND State != @${Params.REQUISITION_STATE}
        """.trimIndent()
      val query =
        makeStatement(sql) {
          bind(Params.MEASUREMENT_CONSUMER_ID to measurementConsumerId.value)
          bind(Params.MEASUREMENT_ID to measurementId.value)
          bind(Params.REQUISITION_STATE to state)
        }
      return transactionContext.executeQuery(query).map { struct ->
        InternalId(struct.getLong("RequisitionId"))
      }
    }
  }
}
