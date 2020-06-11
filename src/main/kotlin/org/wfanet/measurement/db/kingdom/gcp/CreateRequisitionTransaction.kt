package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.TransactionContext
import com.google.cloud.spanner.Value
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.flow.singleOrNull
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.RandomIdGenerator
import org.wfanet.measurement.db.gcp.appendClause
import org.wfanet.measurement.db.gcp.asFlow
import org.wfanet.measurement.db.gcp.spannerDispatcher
import org.wfanet.measurement.db.gcp.toGcpTimestamp
import org.wfanet.measurement.db.gcp.toProtoBytes
import org.wfanet.measurement.db.gcp.toProtoEnum
import org.wfanet.measurement.internal.kingdom.Requisition

/**
 * Persists a Requisition in Spanner if it doesn't yet exist.
 *
 * Idempotency is determined by the Data Provider, Campaign, time window, and RequisitionDetails.
 */
class CreateRequisitionTransaction(private val randomIdGenerator: RandomIdGenerator) {
  data class ParentKey(
    val dataProviderId: Long,
    val campaignId: Long
  )

  /**
   * Runs the transaction body.
   *
   * This does not enforce any preconditions on [requisition]. For example, there is no guarantee
   * that the startTime is before the endTime or the state is valid.
   *
   * @param[transactionContext] the transaction to use
   * @param[requisition] the new [Requisition]
   * @return the existing [Requisition] or null
   */
  fun execute(
    transactionContext: TransactionContext,
    requisition: Requisition
  ): Requisition? = runBlocking(spannerDispatcher()) {
    val existing = findExistingRequisition(transactionContext, requisition)
    if (existing == null) {
      val parentKey = findParentKey(transactionContext, requisition.externalCampaignId)
      transactionContext.buffer(requisition.toInsertMutation(parentKey))
    }
    existing
  }

  private suspend fun findParentKey(
    transactionContext: TransactionContext,
    externalCampaignId: Long
  ): ParentKey {
    val sql =
      """
      SELECT Campaigns.DataProviderId, Campaigns.CampaignId
      FROM Campaigns
      WHERE Campaigns.ExternalCampaignId = @external_campaign_id
      """.trimIndent()

    val statement: Statement =
      Statement.newBuilder(sql)
        .bind("external_campaign_id").to(externalCampaignId)
        .build()

    val row: Struct = transactionContext.executeQuery(statement).asFlow().single()

    return ParentKey(
      row.getLong("DataProviderId"),
      row.getLong("CampaignId")
    )
  }

  private suspend fun findExistingRequisition(
    transactionContext: TransactionContext,
    newRequisition: Requisition
  ): Requisition? {
    val whereClause =
      """
      WHERE DataProviders.ExternalDataProviderId = @external_data_provider_id
        AND Campaigns.ExternalCampaignId = @external_campaign_id
        AND Requisitions.WindowStartTime = @window_start_time
        AND Requisitions.WindowEndTime = @window_end_time
      """.trimIndent()

    return RequisitionReader()
      .withBuilder {
        appendClause(whereClause)
        bind("external_data_provider_id").to(newRequisition.externalDataProviderId)
        bind("external_campaign_id").to(newRequisition.externalCampaignId)
        bind("window_start_time").to(newRequisition.windowStartTime.toGcpTimestamp())
        bind("window_end_time").to(newRequisition.windowEndTime.toGcpTimestamp())
      }
      .execute(transactionContext)
      .map { it.requisition }
      .filter { it.requisitionDetails == newRequisition.requisitionDetails }
      .singleOrNull()
  }

  private fun Requisition.toInsertMutation(parentKey: ParentKey): Mutation =
    Mutation.newInsertBuilder("Requisitions")
      .set("DataProviderId").to(parentKey.dataProviderId)
      .set("CampaignId").to(parentKey.campaignId)
      .set("RequisitionId").to(randomIdGenerator.generateInternalId().value)
      .set("ExternalRequisitionId").to(randomIdGenerator.generateExternalId().value)
      .set("WindowStartTime").to(windowStartTime.toGcpTimestamp())
      .set("WindowEndTime").to(windowEndTime.toGcpTimestamp())
      .set("CreateTime").to(Value.COMMIT_TIMESTAMP)
      .set("State").toProtoEnum(state)
      .set("RequisitionDetails").toProtoBytes(requisitionDetails)
      .set("RequisitionDetailsJson").to(requisitionDetailsJson)
      .build()
}
