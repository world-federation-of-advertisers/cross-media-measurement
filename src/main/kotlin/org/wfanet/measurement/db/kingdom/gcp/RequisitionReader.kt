package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.Struct
import org.wfanet.measurement.db.gcp.getProtoBufMessage
import org.wfanet.measurement.db.gcp.getProtoEnum
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionDetails
import org.wfanet.measurement.internal.kingdom.RequisitionState

/**
 * Reads [Requisition] protos (and internal requisition ids) from Spanner.
 */
class RequisitionReader : SpannerReader<RequisitionAndInternalId>() {
  override val baseSql: String =
    """
    SELECT Requisitions.DataProviderId,
           Requisitions.CampaignId,
           Requisitions.RequisitionId,
           Requisitions.CreateTime,
           Requisitions.ExternalRequisitionId,
           Requisitions.WindowStartTime,
           Requisitions.WindowEndTime,
           Requisitions.State,
           Requisitions.RequisitionDetails,
           Requisitions.RequisitionDetailsJson,
           DataProviders.ExternalDataProviderId,
           Campaigns.ExternalCampaignId
    FROM Requisitions
    JOIN DataProviders USING (DataProviderId)
    JOIN Campaigns USING (DataProviderId, CampaignId)
    """.trimIndent()

  override suspend fun translate(struct: Struct): RequisitionAndInternalId =
    RequisitionAndInternalId(
      buildRequisition(struct),
      struct.getLong("RequisitionId")
    )

  private fun buildRequisition(struct: Struct): Requisition = Requisition.newBuilder().apply {
    externalDataProviderId = struct.getLong("ExternalDataProviderId")
    externalCampaignId = struct.getLong("ExternalCampaignId")
    externalRequisitionId = struct.getLong("ExternalRequisitionId")

    createTime = struct.getTimestamp("CreateTime").toProto()

    windowStartTime = struct.getTimestamp("WindowStartTime").toProto()
    windowEndTime = struct.getTimestamp("WindowEndTime").toProto()

    state = struct.getProtoEnum("State", RequisitionState::forNumber)
    requisitionDetails = struct.getProtoBufMessage(
      "RequisitionDetails", RequisitionDetails.parser()
    )
    requisitionDetailsJson = struct.getString("RequisitionDetailsJson")
  }.build()
}

data class RequisitionAndInternalId(
  val requisition: Requisition,
  val requisitionId: Long
)
