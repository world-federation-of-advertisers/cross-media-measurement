package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.TransactionContext
import kotlinx.coroutines.flow.singleOrNull
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.db.gcp.executeSqlQuery
import org.wfanet.measurement.db.gcp.spannerDispatcher
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionState

/**
 * Marks a Requisition in Spanner as fulfilled (with state [RequisitionState.FULFILLED]).
 */
class FulfillRequisitionTransaction {
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
  ): Requisition =
    updateState(transactionContext, readRequisition(transactionContext, externalRequisitionId))

  private fun readRequisition(
    transactionContext: TransactionContext,
    externalRequisitionId: ExternalId
  ): Requisition {
    val query = readRequisitionQuery(externalRequisitionId)
    val struct: Struct? = runBlocking(spannerDispatcher()) {
      transactionContext.executeSqlQuery(query).singleOrNull()
    }
    return struct?.toRequisition()
      ?: throw IllegalArgumentException("Requisition $externalRequisitionId not found")
  }

  private fun readRequisitionQuery(externalRequisitionId: ExternalId): Statement =
    REQUISITION_READ_QUERY
      .toBuilder()
      .append("WHERE ExternalRequisitionId = @external_requisition_id")
      .bind("external_requisition_id").to(externalRequisitionId.value)
      .build()

  private fun updateState(
    transactionContext: TransactionContext,
    originalRequisition: Requisition
  ): Requisition {
    val updatedRequisition = setStateFulfilled(originalRequisition)
    val mutation = updateStateMutation(updatedRequisition)
    runBlocking(spannerDispatcher()) {
      transactionContext.buffer(mutation)
    }
    return updatedRequisition
  }

  private fun setStateFulfilled(requisition: Requisition): Requisition =
    requisition
      .toBuilder()
      .setState(RequisitionState.FULFILLED)
      .build()

  private fun updateStateMutation(requisition: Requisition) =
    Mutation
      .newUpdateBuilder("Requisitions")
      .addPrimaryKey(requisition)
      .set("State").to(requisition.state.ordinal.toLong())
      .build()
}
