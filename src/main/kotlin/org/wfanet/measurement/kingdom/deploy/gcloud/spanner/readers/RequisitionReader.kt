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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers

import com.google.cloud.spanner.Struct
import org.wfanet.measurement.gcloud.spanner.getProtoEnum
import org.wfanet.measurement.gcloud.spanner.getProtoMessage
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState
import org.wfanet.measurement.internal.kingdom.RequisitionDetails

/**
 * Reads [Requisition] protos (and their internal primary keys) from Spanner.
 */
class RequisitionReader : SpannerReader<RequisitionReader.Result>() {
  data class Result(
    val requisition: Requisition,
    val dataProviderId: Long,
    val campaignId: Long,
    val requisitionId: Long
  )

  override val baseSql: String =
    """
    SELECT Requisitions.DataProviderId,
           Requisitions.CampaignId,
           Requisitions.RequisitionId,
           Requisitions.CreateTime,
           Requisitions.ExternalRequisitionId,
           Requisitions.CombinedPublicKeyResourceId,
           Requisitions.WindowStartTime,
           Requisitions.WindowEndTime,
           Requisitions.State,
           Requisitions.DuchyId,
           Requisitions.RequisitionDetails,
           Requisitions.RequisitionDetailsJson,
           DataProviders.ExternalDataProviderId,
           Campaigns.ExternalCampaignId,
           Campaigns.ProvidedCampaignId
    FROM Requisitions
    JOIN DataProviders USING (DataProviderId)
    JOIN Campaigns USING (DataProviderId, CampaignId)
    """.trimIndent()

  override val externalIdColumn: String = "Requisitions.ExternalRequisitionId"

  override suspend fun translate(struct: Struct): Result =
    Result(
      requisition = buildRequisition(struct),
      dataProviderId = struct.getLong("DataProviderId"),
      campaignId = struct.getLong("CampaignId"),
      requisitionId = struct.getLong("RequisitionId")
    )

  private fun buildRequisition(struct: Struct): Requisition = Requisition.newBuilder().apply {
    externalDataProviderId = struct.getLong("ExternalDataProviderId")
    externalCampaignId = struct.getLong("ExternalCampaignId")
    externalRequisitionId = struct.getLong("ExternalRequisitionId")
    combinedPublicKeyResourceId = struct.getString("CombinedPublicKeyResourceId")

    providedCampaignId = struct.getString("ProvidedCampaignId")

    createTime = struct.getTimestamp("CreateTime").toProto()

    windowStartTime = struct.getTimestamp("WindowStartTime").toProto()
    windowEndTime = struct.getTimestamp("WindowEndTime").toProto()

    state = struct.getProtoEnum("State", RequisitionState::forNumber)

    if (!struct.isNull("DuchyId")) {
      duchyId = struct.getString("DuchyId")
    }

    requisitionDetails = struct.getProtoMessage(
      "RequisitionDetails",
      RequisitionDetails.parser()
    )
    requisitionDetailsJson = struct.getString("RequisitionDetailsJson")
  }.build()
}
