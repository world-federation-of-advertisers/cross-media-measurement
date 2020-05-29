package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.Struct
import org.wfanet.measurement.db.gcp.getBytesAsByteArray
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionDetails
import org.wfanet.measurement.internal.kingdom.RequisitionState

fun Struct.toRequisition(): Requisition {
  return Requisition.newBuilder().apply {
    externalDataProviderId = getLong("DataProviderId")
    externalCampaignId = getLong("CampaignId")
    externalRequisitionId = getLong("RequisitionId")

    createTime = getTimestamp("CreateTime").toProto()
    externalDataProviderId = getLong("ExternalDataProviderId")
    externalCampaignId = getLong("ExternalCampaignId")
    externalRequisitionId = getLong("ExternalRequisitionId")

    windowStartTime = getTimestamp("WindowStartTime").toProto()
    windowEndTime = getTimestamp("WindowEndTime").toProto()

    state = RequisitionState.forNumber(getLong("State").toInt())
    requisitionDetails =
      RequisitionDetails.parseFrom(getBytesAsByteArray("RequisitionDetails"))
    requisitionDetailsJson = getString("RequisitionDetailsJson")
  }.build()
}
