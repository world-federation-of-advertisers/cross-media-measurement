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

import com.google.cloud.spanner.TimestampBound
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.singleOrNullIfEmpty
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.gcloud.spanner.getInternalId
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.gcloud.spanner.to
import org.wfanet.measurement.gcloud.spanner.toInt64
import org.wfanet.measurement.internal.kingdom.FulfillRequisitionRequest
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.ProtocolConfig
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionDetails
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.measurementLogEntryDetails
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuchyNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.RequisitionNotFoundByComputationException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.RequisitionNotFoundByDataProviderException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.RequisitionStateIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.RequisitionInternalKey
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
 * @throws [RequisitionStateIllegalException] Requisition state is not UNFULFILLED
 * @throws [RequisitionNotFoundByComputationException] Requisition not found
 * @throws [RequisitionNotFoundByDataProviderException] Requisition not found
 * @throws [DuchyNotFoundException] Duchy not found
 */
class FulfillRequisition(private val request: FulfillRequisitionRequest) :
  BaseSpannerWriter<Requisition> {
  private data class RequisitionResult(
    val key: RequisitionInternalKey,
    val state: Requisition.State,
    val details: RequisitionDetails,
    val measurementState: Measurement.State,
    val protocol: ProtocolConfig.ProtocolCase,
  )

  override suspend fun execute(
    databaseClient: AsyncDatabaseClient,
    idGenerator: IdGenerator,
  ): Requisition {
    val writeTxnRunner: AsyncDatabaseClient.TransactionRunner =
      databaseClient.readWriteTransaction()
    val key: RequisitionInternalKey =
      writeTxnRunner.run { txn -> SpannerWriter.TransactionScope(txn, idGenerator).executeWrite() }

    val readTxn: AsyncDatabaseClient.ReadContext =
      databaseClient.singleUse(
        TimestampBound.ofMinReadTimestamp(writeTxnRunner.getCommitTimestamp())
      )
    return checkNotNull(RequisitionReader.readByKey(readTxn, key)).requisition
  }

  private suspend fun SpannerWriter.TransactionScope.executeWrite(): RequisitionInternalKey {
    val requisition: RequisitionResult = txn.readRequisition()
    val (measurementConsumerId: InternalId, measurementId: InternalId, requisitionId: InternalId) =
      requisition.key
    val state: Requisition.State = requisition.state

    if (state != Requisition.State.UNFULFILLED) {
      throw RequisitionStateIllegalException(ExternalId(request.externalRequisitionId), state) {
        "Expected ${Requisition.State.UNFULFILLED}, got $state"
      }
    }
    val measurementState = requisition.measurementState
    check(measurementState == Measurement.State.PENDING_REQUISITION_FULFILLMENT) {
      error("Unexpected measurement state $measurementState")
    }

    val updatedDetails =
      requisition.details.copy {
        nonce = request.nonce
        if (request.hasDirectParams()) {
          encryptedData = request.directParams.encryptedData
          externalCertificateId = request.directParams.externalCertificateId
          encryptedDataApiVersion = request.directParams.apiVersion
        }
      }

    val nonFulfilledRequisitionIds =
      txn.readRequisitionsNotInState(
        measurementConsumerId,
        measurementId,
        Requisition.State.FULFILLED,
      )
    if (nonFulfilledRequisitionIds.singleOrNull() == requisitionId) {
      val nextState: Measurement.State =
        when (requisition.protocol) {
          ProtocolConfig.ProtocolCase.DIRECT -> Measurement.State.SUCCEEDED
          ProtocolConfig.ProtocolCase.LIQUID_LEGIONS_V2,
          ProtocolConfig.ProtocolCase.REACH_ONLY_LIQUID_LEGIONS_V2 ->
            Measurement.State.PENDING_PARTICIPANT_CONFIRMATION
          ProtocolConfig.ProtocolCase.HONEST_MAJORITY_SHARE_SHUFFLE ->
            Measurement.State.PENDING_COMPUTATION
          ProtocolConfig.ProtocolCase.PROTOCOL_NOT_SET -> error("protocol not set")
        }
      val measurementLogEntryDetails = measurementLogEntryDetails {
        logMessage = "All requisitions fulfilled"
      }
      // All other Requisitions are already FULFILLED, so update Measurement state.
      updateMeasurementState(
        measurementConsumerId = measurementConsumerId,
        measurementId = measurementId,
        nextState = nextState,
        previousState = measurementState,
        measurementLogEntryDetails = measurementLogEntryDetails,
      )
    }

    val fulfillDuchyId = if (request.hasComputedParams()) getFulfillDuchyId() else null
    txn.updateRequisition(
      requisition.key,
      Requisition.State.FULFILLED,
      updatedDetails,
      fulfillDuchyId,
    )

    return requisition.key
  }

  private suspend fun AsyncDatabaseClient.ReadContext.readRequisition(): RequisitionResult {
    val externalRequisitionId = ExternalId(request.externalRequisitionId)
    val query =
      statement(SELECT) {
        bind("externalRequisitionId").to(externalRequisitionId)
        if (request.hasComputedParams()) {
          appendClause(
            """
          FROM
            Measurements
            JOIN Requisitions USING (MeasurementConsumerId, MeasurementId)
          WHERE
            ExternalComputationId = @externalComputationId
            AND ExternalRequisitionId = @externalRequisitionId
          """
              .trimIndent()
          )
          bind("externalComputationId").to(request.computedParams.externalComputationId)
        } else {
          appendClause(
            """
          FROM
            Requisitions
            JOIN DataProviders USING (DataProviderId)
            JOIN Measurements USING (MeasurementConsumerId, MeasurementId)
          WHERE
            ExternalDataProviderId = @externalDataProviderId
            AND ExternalRequisitionId = @externalRequisitionId
          """
              .trimIndent()
          )
          bind("externalDataProviderId").to(request.directParams.externalDataProviderId)
        }
      }

    val row =
      executeQuery(query).singleOrNullIfEmpty()
        ?: if (request.hasComputedParams()) {
          throw RequisitionNotFoundByComputationException(
            ExternalId(request.computedParams.externalComputationId),
            externalRequisitionId,
          )
        } else {
          throw RequisitionNotFoundByDataProviderException(
            ExternalId(request.directParams.externalDataProviderId),
            externalRequisitionId,
          )
        }

    val key =
      RequisitionInternalKey(
        row.getInternalId("MeasurementConsumerId"),
        row.getInternalId("MeasurementId"),
        row.getInternalId("RequisitionId"),
      )
    val protocolConfig = row.getProtoMessage("ProtocolConfig", ProtocolConfig.getDefaultInstance())
    return RequisitionResult(
      key,
      row.getProtoEnum("State", Requisition.State::forNumber),
      row.getProtoMessage("RequisitionDetails", RequisitionDetails.getDefaultInstance()),
      row.getProtoEnum("MeasurementState", Measurement.State::forNumber),
      protocolConfig.protocolCase,
    )
  }

  private fun getFulfillDuchyId(): InternalId {
    val externalDuchyId: String = request.computedParams.externalFulfillingDuchyId
    return DuchyIds.getInternalId(externalDuchyId)?.let { InternalId(it) }
      ?: throw DuchyNotFoundException(externalDuchyId) {
        "Duchy with external ID $externalDuchyId not found"
      }
  }

  companion object {
    private val SELECT =
      """
      SELECT
        MeasurementConsumerId,
        MeasurementId,
        RequisitionId,
        Requisitions.State,
        RequisitionDetails,
        Measurements.State AS MeasurementState,
        MeasurementDetails.protocol_config AS ProtocolConfig,
      """
        .trimIndent()

    private fun AsyncDatabaseClient.ReadContext.readRequisitionsNotInState(
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
      return executeQuery(query).map { struct -> InternalId(struct.getLong("RequisitionId")) }
    }
  }
}
