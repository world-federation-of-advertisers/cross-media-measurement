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

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.kingdom.FulfillRequisitionRequest
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntryKt
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuchyNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementStateIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.RequisitionNotFoundByComputationException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.RequisitionNotFoundByDataProviderException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.RequisitionStateIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.RequisitionReader

private object Params {
  const val MEASUREMENT_CONSUMER_ID = "measurementConsumerId"
  const val MEASUREMENT_ID = "measurementId"
  const val REQUISITION_STATE = "requisitionState"
}

/**
 * Fulfills a [Requisition].
 *
 * Throws a subclass of [KingdomInternalException] on [execute].
 *
 * @throws [MeasurementStateIllegalException] Measurement state is not
 *   PENDING_REQUISITION_FULFILLMENT
 * @throws [RequisitionStateIllegalException] Requisition state is not UNFULFILLED
 * @throws [RequisitionNotFoundByComputationException] Requisition not found
 * @throws [RequisitionNotFoundByDataProviderException] Requisition not found
 * @throws [DuchyNotFoundException] Duchy not found
 */
class FulfillRequisition(private val request: FulfillRequisitionRequest) :
  SpannerWriter<Requisition, Requisition>() {
  override suspend fun TransactionScope.runTransaction(): Requisition {
    val readResult: RequisitionReader.Result = readRequisition()
    val (measurementConsumerId, measurementId, requisitionId, requisition) = readResult

    val state = requisition.state
    if (state != Requisition.State.UNFULFILLED) {
      throw RequisitionStateIllegalException(ExternalId(request.externalRequisitionId), state) {
        "Expected ${Requisition.State.UNFULFILLED}, got $state"
      }
    }
    val measurementState = requisition.parentMeasurement.state
    if (measurementState != Measurement.State.PENDING_REQUISITION_FULFILLMENT) {
      throw MeasurementStateIllegalException(
        ExternalId(requisition.externalMeasurementConsumerId),
        ExternalId(requisition.externalMeasurementId),
        measurementState
      ) {
        "Expected ${Measurement.State.PENDING_REQUISITION_FULFILLMENT}, got $measurementState"
      }
    }

    val updatedDetails =
      requisition.details.copy {
        nonce = request.nonce
        if (request.hasDirectParams()) {
          encryptedData = request.directParams.encryptedData
        }
      }

    val nonFulfilledRequisitionIds =
      readRequisitionsNotInState(measurementConsumerId, measurementId, Requisition.State.FULFILLED)
    val updatedMeasurementState: Measurement.State? =
      if (nonFulfilledRequisitionIds.singleOrNull() == requisitionId) {
        val nextState =
          if (request.hasComputedParams()) Measurement.State.PENDING_PARTICIPANT_CONFIRMATION
          else Measurement.State.SUCCEEDED
        val measurementLogEntryDetails =
          MeasurementLogEntryKt.details { logMessage = "All requisitions fulfilled" }
        // All other Requisitions are already FULFILLED, so update Measurement state.
        nextState.also {
          updateMeasurementState(
            measurementConsumerId = measurementConsumerId,
            measurementId = measurementId,
            nextState = it,
            previousState = measurementState,
            logDetails = measurementLogEntryDetails
          )
        }
      } else {
        null
      }

    val fulfillDuchyId = if (request.hasComputedParams()) getFulfillDuchyId() else null
    updateRequisition(readResult, Requisition.State.FULFILLED, updatedDetails, fulfillDuchyId)

    return requisition.copy {
      if (request.hasComputedParams()) {
        externalFulfillingDuchyId = request.computedParams.externalFulfillingDuchyId
      }
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
    val externalRequisitionId = request.externalRequisitionId
    if (request.hasComputedParams()) {
      val externalComputationId = request.computedParams.externalComputationId
      return RequisitionReader()
        .readByExternalComputationId(
          transactionContext,
          externalComputationId = externalComputationId,
          externalRequisitionId = externalRequisitionId
        )
        ?: throw RequisitionNotFoundByComputationException(
          ExternalId(externalComputationId),
          ExternalId(externalRequisitionId)
        ) {
          "Requisition with external Computation ID $externalComputationId and external " +
            "Requisition ID $externalRequisitionId not found"
        }
    } else {
      val externalDataProviderId = request.directParams.externalDataProviderId
      return RequisitionReader()
        .readByExternalDataProviderId(
          transactionContext,
          externalDataProviderId = externalDataProviderId,
          externalRequisitionId = externalRequisitionId
        )
        ?: throw RequisitionNotFoundByDataProviderException(
          ExternalId(externalDataProviderId),
          ExternalId(externalRequisitionId)
        ) {
          "Requisition with external DataProvider ID $externalDataProviderId and external " +
            "Requisition ID $externalRequisitionId not found"
        }
    }
  }

  private fun getFulfillDuchyId(): InternalId {
    val externalDuchyId: String = request.computedParams.externalFulfillingDuchyId
    return DuchyIds.getInternalId(externalDuchyId)?.let { InternalId(it) }
      ?: throw DuchyNotFoundException(externalDuchyId) {
        "Duchy with external ID $externalDuchyId not found"
      }
  }

  companion object {
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
        """
          .trimIndent()
      val query =
        statement(sql) {
          bind(Params.MEASUREMENT_CONSUMER_ID to measurementConsumerId)
          bind(Params.MEASUREMENT_ID to measurementId)
          bind(Params.REQUISITION_STATE to state)
        }
      return transactionContext.executeQuery(query).map { struct ->
        InternalId(struct.getLong("RequisitionId"))
      }
    }
  }
}
