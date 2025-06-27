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
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.gcloud.spanner.toInt64
import org.wfanet.measurement.internal.kingdom.FulfillRequisitionRequest
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.measurementLogEntryDetails
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuchyNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ETags
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementStateIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.RequisitionEtagMismatchException
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

    if (request.etag.isNotEmpty()) {
      val currentEtag = ETags.computeETag(requisition.updateTime.toGcloudTimestamp())
      if (request.etag != currentEtag) {
        throw RequisitionEtagMismatchException(request.etag, currentEtag)
      }
    }

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
        measurementState,
      ) {
        "Expected ${Measurement.State.PENDING_REQUISITION_FULFILLMENT}, got $measurementState"
      }
    }

    val updatedDetails =
      requisition.details.copy {
        nonce = request.nonce
        if (request.hasDirectParams()) {
          encryptedData = request.directParams.encryptedData
          externalCertificateId = request.directParams.externalCertificateId
          encryptedDataApiVersion = request.directParams.apiVersion
        }
        if (request.hasFulfillmentContext()) {
          fulfillmentContext = request.fulfillmentContext
        }
      }

    val nonFulfilledRequisitionIds =
      readRequisitionsNotInState(measurementConsumerId, measurementId, Requisition.State.FULFILLED)
    val updatedMeasurementState: Measurement.State? =
      if (nonFulfilledRequisitionIds.singleOrNull() == requisitionId) {
        val nextState =
          if (request.hasComputedParams()) {
            if (requisition.parentMeasurement.protocolConfig.hasHonestMajorityShareShuffle()) {
              Measurement.State.PENDING_COMPUTATION
            } else {
              Measurement.State.PENDING_PARTICIPANT_CONFIRMATION
            }
          } else Measurement.State.SUCCEEDED
        val measurementLogEntryDetails = measurementLogEntryDetails {
          logMessage = "All requisitions fulfilled"
        }
        // All other Requisitions are already FULFILLED, so update Measurement state.
        nextState.also {
          updateMeasurementState(
            measurementConsumerId = measurementConsumerId,
            measurementId = measurementId,
            nextState = it,
            previousState = measurementState,
            measurementLogEntryDetails = measurementLogEntryDetails,
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
    return checkNotNull(transactionResult).copy {
      updateTime = commitTimestamp.toProto()
      etag = ETags.computeETag(commitTimestamp)
    }
  }

  private suspend fun TransactionScope.readRequisition(): RequisitionReader.Result {
    val externalRequisitionId = ExternalId(request.externalRequisitionId)
    if (request.hasComputedParams()) {
      val externalComputationId = ExternalId(request.computedParams.externalComputationId)
      return RequisitionReader.readByExternalComputationId(
        transactionContext,
        externalComputationId = externalComputationId,
        externalRequisitionId = externalRequisitionId,
      )
        ?: throw RequisitionNotFoundByComputationException(
          externalComputationId,
          externalRequisitionId,
        )
    } else {
      val externalDataProviderId = ExternalId(request.directParams.externalDataProviderId)
      return RequisitionReader.readByExternalDataProviderId(
        transactionContext,
        externalDataProviderId = externalDataProviderId,
        externalRequisitionId = externalRequisitionId,
      )
        ?: throw RequisitionNotFoundByDataProviderException(
          externalDataProviderId,
          externalRequisitionId,
        )
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
      state: Requisition.State,
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
          bind(Params.REQUISITION_STATE).toInt64(state)
        }
      return transactionContext
        .executeQuery(
          query,
          Options.tag("writer=FulfillRequisition,action=readRequisitionsNotInState"),
        )
        .map { struct -> InternalId(struct.getLong("RequisitionId")) }
    }
  }
}
