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

package org.wfanet.measurement.db.kingdom.gcp.writers

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.InternalId
import org.wfanet.measurement.db.gcp.appendClause
import org.wfanet.measurement.db.gcp.asFlow
import org.wfanet.measurement.db.gcp.bufferTo
import org.wfanet.measurement.db.gcp.toProtoBytes
import org.wfanet.measurement.db.gcp.toProtoEnum
import org.wfanet.measurement.db.kingdom.gcp.readers.RequisitionReader
import org.wfanet.measurement.gcloud.toGcloudTimestamp
import org.wfanet.measurement.internal.kingdom.Requisition

/**
 * Persists a Requisition in Spanner if it doesn't yet exist.
 *
 * Idempotency is determined by the Data Provider, Campaign, time window, and RequisitionDetails.
 *
 * This does not enforce any preconditions on [requisition]. For example, there is no guarantee
 * that the startTime is before the endTime or the state is valid.
 */
class CreateRequisition(
  private val requisition: Requisition
) : SpannerWriter<Requisition, Requisition>() {
  data class ParentKey(
    val dataProviderId: Long,
    val campaignId: Long
  )

  override suspend fun TransactionScope.runTransaction(): Requisition {
    val existing = findExistingRequisition()
    if (existing != null) {
      return existing
    }

    val parentKey = findParentKey(requisition.externalCampaignId)
    val externalRequisitionId = idGenerator.generateExternalId()
    requisition
      .toInsertMutation(parentKey, idGenerator.generateInternalId(), externalRequisitionId)
      .bufferTo(transactionContext)

    return requisition.toBuilder().apply {
      this.externalRequisitionId = externalRequisitionId.value
    }.build()
  }

  override fun ResultScope<Requisition>.buildResult(): Requisition {
    return checkNotNull(transactionResult).toBuilder().apply {
      createTime = commitTimestamp.toProto()
    }.build()
  }

  private suspend fun TransactionScope.findParentKey(externalCampaignId: Long): ParentKey {
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

  private suspend fun TransactionScope.findExistingRequisition(): Requisition? {
    val whereClause =
      """
      WHERE DataProviders.ExternalDataProviderId = @external_data_provider_id
        AND Campaigns.ExternalCampaignId = @external_campaign_id
        AND Requisitions.CombinedPublicKeyResourceId = @combined_public_key_resource_id
        AND Requisitions.WindowStartTime = @window_start_time
        AND Requisitions.WindowEndTime = @window_end_time
      """.trimIndent()

    return RequisitionReader()
      .withBuilder {
        appendClause(whereClause)
        bind("external_data_provider_id").to(requisition.externalDataProviderId)
        bind("external_campaign_id").to(requisition.externalCampaignId)
        bind("combined_public_key_resource_id").to(requisition.combinedPublicKeyResourceId)
        bind("window_start_time").to(requisition.windowStartTime.toGcloudTimestamp())
        bind("window_end_time").to(requisition.windowEndTime.toGcloudTimestamp())
      }
      .execute(transactionContext)
      .map { it.requisition }
      .filter { it.requisitionDetails == requisition.requisitionDetails }
      .singleOrNull()
  }

  private fun Requisition.toInsertMutation(
    parentKey: ParentKey,
    requisitionId: InternalId,
    externalRequisitionId: ExternalId
  ): Mutation =
    Mutation.newInsertBuilder("Requisitions")
      .set("DataProviderId").to(parentKey.dataProviderId)
      .set("CampaignId").to(parentKey.campaignId)
      .set("RequisitionId").to(requisitionId.value)
      .set("ExternalRequisitionId").to(externalRequisitionId.value)
      .set("CombinedPublicKeyResourceId").to(combinedPublicKeyResourceId)
      .set("WindowStartTime").to(windowStartTime.toGcloudTimestamp())
      .set("WindowEndTime").to(windowEndTime.toGcloudTimestamp())
      .set("CreateTime").to(Value.COMMIT_TIMESTAMP)
      .set("State").toProtoEnum(state)
      .set("RequisitionDetails").toProtoBytes(requisitionDetails)
      .set("RequisitionDetailsJson").to(requisitionDetailsJson)
      .build()
}
