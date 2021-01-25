// Copyright 2020 The Cross-Media Measurement Authors
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

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bufferTo
import org.wfanet.measurement.gcloud.spanner.toProtoBytes
import org.wfanet.measurement.gcloud.spanner.toProtoEnum
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.RequisitionReader

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
  data class Parent(
    val dataProviderId: Long,
    val campaignId: Long,
    val providedCampaignId: String
  )

  override suspend fun TransactionScope.runTransaction(): Requisition {
    val existing = findExistingRequisition()
    if (existing != null) {
      return existing
    }

    val parent = findParent(requisition.externalCampaignId)
    val actualRequisition = requisition.toBuilder().apply {
      externalRequisitionId = idGenerator.generateExternalId().value
      providedCampaignId = parent.providedCampaignId
      state = Requisition.RequisitionState.UNFULFILLED
    }.build()
    actualRequisition
      .toInsertMutation(parent, idGenerator.generateInternalId())
      .bufferTo(transactionContext)

    return actualRequisition
  }

  override fun ResultScope<Requisition>.buildResult(): Requisition {
    val requisition = checkNotNull(transactionResult)
    return if (requisition.hasCreateTime()) {
      requisition
    } else {
      requisition.toBuilder().apply {
        createTime = commitTimestamp.toProto()
      }.build()
    }
  }

  private suspend fun TransactionScope.findParent(externalCampaignId: Long): Parent {
    val sql =
      """
      SELECT Campaigns.DataProviderId, Campaigns.CampaignId, Campaigns.ProvidedCampaignId
      FROM Campaigns
      WHERE Campaigns.ExternalCampaignId = @external_campaign_id
      """.trimIndent()

    val statement: Statement =
      Statement.newBuilder(sql)
        .bind("external_campaign_id").to(externalCampaignId)
        .build()

    val row: Struct = transactionContext.executeQuery(statement).single()

    return Parent(
      row.getLong("DataProviderId"),
      row.getLong("CampaignId"),
      row.getString("ProvidedCampaignId")
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

  private fun Requisition.toInsertMutation(parent: Parent, requisitionId: InternalId): Mutation {
    return Mutation.newInsertBuilder("Requisitions")
      .set("DataProviderId").to(parent.dataProviderId)
      .set("CampaignId").to(parent.campaignId)
      .set("RequisitionId").to(requisitionId.value)
      .set("ExternalRequisitionId").to(externalRequisitionId)
      .set("CombinedPublicKeyResourceId").to(combinedPublicKeyResourceId)
      .set("WindowStartTime").to(windowStartTime.toGcloudTimestamp())
      .set("WindowEndTime").to(windowEndTime.toGcloudTimestamp())
      .set("CreateTime").to(Value.COMMIT_TIMESTAMP)
      .set("State").toProtoEnum(state)
      .set("RequisitionDetails").toProtoBytes(requisitionDetails)
      .set("RequisitionDetailsJson").to(requisitionDetailsJson)
      .build()
  }
}
