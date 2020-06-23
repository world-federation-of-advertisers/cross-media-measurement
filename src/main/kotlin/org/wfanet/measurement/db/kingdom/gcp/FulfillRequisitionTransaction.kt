package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.TransactionContext
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.db.gcp.toProtoEnum
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState

/**
 * Marks a Requisition in Spanner as fulfilled (with state [RequisitionState.FULFILLED]).
 */
class FulfillRequisitionTransaction {
  private data class ReadResult(
    val requisitionId: Long,
    val requisition: Requisition
  )
  /**
   * Runs the transaction body.
   *
   * @param[transactionContext] the transaction to use
   * @param[externalRequisitionId] the id of the [Requisition]
   * @throws[IllegalArgumentException] if the [Requisition] doesn't exist
   * @return the existing [Requisition] after being marked as fulfilled
   */
  fun execute(
    transactionContext: TransactionContext,
    externalRequisitionId: ExternalId
  ): Requisition {
    val readResult = runBlocking { readRequisition(transactionContext, externalRequisitionId) }
    updateState(transactionContext, readResult)
    return readResult.requisition.toBuilder().setState(RequisitionState.FULFILLED).build()
  }

  private suspend fun readRequisition(
    transactionContext: TransactionContext,
    externalRequisitionId: ExternalId
  ): RequisitionReadResult =
    RequisitionReader()
      .withBuilder {
        append("WHERE ExternalRequisitionId = @external_requisition_id")
        bind("external_requisition_id").to(externalRequisitionId.value)
      }
      .execute(transactionContext)
      .single()

  private fun updateState(
    transactionContext: TransactionContext,
    readResult: RequisitionReadResult
  ) {
    val mutation: Mutation =
      Mutation.newUpdateBuilder("Requisitions")
        .set("DataProviderId").to(readResult.dataProviderId)
        .set("CampaignId").to(readResult.campaignId)
        .set("RequisitionId").to(readResult.requisitionId)
        .set("State").toProtoEnum(RequisitionState.FULFILLED)
        .build()
    transactionContext.buffer(mutation)
  }
}
