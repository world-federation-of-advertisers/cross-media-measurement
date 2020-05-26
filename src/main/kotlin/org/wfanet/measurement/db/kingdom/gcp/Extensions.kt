package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Struct
import org.wfanet.measurement.db.gcp.getBytesAsByteArray
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionDetails
import org.wfanet.measurement.internal.kingdom.RequisitionState

fun Mutation.WriteBuilder.addPrimaryKey(requisition: Requisition): Mutation.WriteBuilder {
  set("DataProviderId").to(requisition.dataProviderId)
  set("CampaignId").to(requisition.campaignId)
  set("RequisitionId").to(requisition.requisitionId)
  return this
}

fun Struct.toRequisition(): Requisition {
  return Requisition.newBuilder().apply {
    dataProviderId = getLong("DataProviderId")
    campaignId = getLong("CampaignId")
    requisitionId = getLong("RequisitionId")

    externalDataProviderId = getLong("ExternalDataProviderId")
    externalCampaignId = getLong("ExternalCampaignId")
    externalRequisitionId = getLong("ExternalRequisitionId")

    windowStartTime = getTimestamp("WindowStartTime").toProto()
    windowEndTime = getTimestamp("WindowEndTime").toProto()

    state = RequisitionState.forNumber(getLong("State").toInt())
    requisitionDetails = RequisitionDetails.parseFrom(getBytesAsByteArray("RequisitionDetails"))
    requisitionDetailsJson = getString("RequisitionDetailsJson")
  }.build()
}
