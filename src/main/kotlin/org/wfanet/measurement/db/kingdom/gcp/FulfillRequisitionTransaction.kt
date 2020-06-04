package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.TransactionContext
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.db.gcp.asFlow
import org.wfanet.measurement.db.gcp.spannerDispatcher
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionState

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
    val readResult = readRequisition(transactionContext, externalRequisitionId)
    updateState(transactionContext, readResult.requisitionId)
    return readResult.requisition.withStateFulfilled()
  }

  private fun readRequisition(
    transactionContext: TransactionContext,
    externalRequisitionId: ExternalId
  ): ReadResult {
    val query = readRequisitionQuery(externalRequisitionId)
    val struct: Struct = runBlocking { transactionContext.executeQuery(query).asFlow().single() }
    return ReadResult(struct.getLong("RequisitionId"), struct.toRequisition())
  }

  private fun readRequisitionQuery(externalRequisitionId: ExternalId): Statement =
    REQUISITION_READ_QUERY
      .toBuilder()
      .append("WHERE ExternalRequisitionId = @external_requisition_id")
      .bind("external_requisition_id").to(externalRequisitionId.value)
      .build()

  private fun updateState(
    transactionContext: TransactionContext,
    requisitionId: Long
  ) {
    val dml = updateStateDml(requisitionId)
    runBlocking(spannerDispatcher()) {
      val rows: Long = transactionContext.executeUpdate(dml)
      require(rows <= 1L) {
        "Unexpected number of rows updated ($rows rows) from query $dml"
      }
    }
  }

  private fun Requisition.withStateFulfilled(): Requisition =
    toBuilder()
      .setState(RequisitionState.FULFILLED)
      .build()

  private fun updateStateDml(requisitionId: Long): Statement {
    val sql =
      """
      UPDATE Requisitions
      SET State = @state
      WHERE RequisitionId = @requisition_id
      """.trimIndent()
    return Statement.newBuilder(sql)
      .bind("state").to(RequisitionState.FULFILLED_VALUE.toLong())
      .bind("requisition_id").to(requisitionId)
      .build()
  }
}
