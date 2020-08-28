// Copyright 2020 The Measurement System Authors
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
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.IdGenerator
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
class CreateRequisitionTransaction(private val idGenerator: IdGenerator) {
  data class ParentKey(
    val dataProviderId: Long,
    val campaignId: Long
  )

  sealed class Result {
    data class ExistingRequisition(val requisition: Requisition) : Result()
    data class NewRequisitionId(val externalRequisitionId: ExternalId) : Result()
  }

  /**
   * Runs the transaction body.
   *
   * This does not enforce any preconditions on [requisition]. For example, there is no guarantee
   * that the startTime is before the endTime or the state is valid.
   *
   * TODO: instead of returning a [Result], construct the new [Requisition] without an additional
   * Spanner read.
   *
   * @param transactionContext the transaction to use
   * @param requisition the new [Requisition]
   * @return the existing [Requisition] or the external id of a newly created Requisition.
   */
  fun execute(
    transactionContext: TransactionContext,
    requisition: Requisition
  ): Result = runBlocking(spannerDispatcher()) {
    val existing = findExistingRequisition(transactionContext, requisition)
    if (existing != null) {
      return@runBlocking Result.ExistingRequisition(existing)
    }

    val parentKey = findParentKey(transactionContext, requisition.externalCampaignId)
    val externalRequisitionId = idGenerator.generateExternalId()
    transactionContext.buffer(requisition.toInsertMutation(parentKey, externalRequisitionId))
    Result.NewRequisitionId(externalRequisitionId)
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

  private fun Requisition.toInsertMutation(
    parentKey: ParentKey,
    externalRequisitionId: ExternalId
  ): Mutation =
    Mutation.newInsertBuilder("Requisitions")
      .set("DataProviderId").to(parentKey.dataProviderId)
      .set("CampaignId").to(parentKey.campaignId)
      .set("RequisitionId").to(idGenerator.generateInternalId().value)
      .set("ExternalRequisitionId").to(externalRequisitionId.value)
      .set("WindowStartTime").to(windowStartTime.toGcpTimestamp())
      .set("WindowEndTime").to(windowEndTime.toGcpTimestamp())
      .set("CreateTime").to(Value.COMMIT_TIMESTAMP)
      .set("State").toProtoEnum(state)
      .set("RequisitionDetails").toProtoBytes(requisitionDetails)
      .set("RequisitionDetailsJson").to(requisitionDetailsJson)
      .build()
}
