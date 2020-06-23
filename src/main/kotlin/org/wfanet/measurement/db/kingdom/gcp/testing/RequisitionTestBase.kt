package org.wfanet.measurement.db.kingdom.gcp.testing

import com.google.cloud.ByteArray
import com.google.cloud.Timestamp
import com.google.cloud.spanner.Mutation
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.db.gcp.toGcpTimestamp
import org.wfanet.measurement.db.gcp.toProtoBytes
import org.wfanet.measurement.db.gcp.toProtoEnum
import org.wfanet.measurement.db.gcp.toProtoJson
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState
import org.wfanet.measurement.internal.kingdom.RequisitionDetails

abstract class RequisitionTestBase : KingdomDatabaseTestBase() {
  companion object {
    const val DATA_PROVIDER_ID = 1L
    const val EXTERNAL_DATA_PROVIDER_ID = 101L
    const val CAMPAIGN_ID = 2L
    const val EXTERNAL_CAMPAIGN_ID = 102L
    const val REQUISITION_ID = 3L
    const val EXTERNAL_REQUISITION_ID = 103L
    const val IRRELEVANT_ADVERTISER_ID = 99L

    val START_TIME: Timestamp = Timestamp.ofTimeSecondsAndNanos(123, 0)
    val END_TIME: Timestamp = Timestamp.ofTimeSecondsAndNanos(456, 0)
    val DETAILS: RequisitionDetails = RequisitionDetails.newBuilder().apply {
      metricDefinitionBuilder.sketchBuilder.sketchConfigId = 10101
    }.build()

    val NEW_TIMESTAMP: Timestamp = Timestamp.ofTimeSecondsAndNanos(999, 0)
    const val NEW_REQUISITION_ID = 555L
    const val NEW_EXTERNAL_REQUISITION_ID = 5555L
    val NEW_DETAILS: RequisitionDetails = RequisitionDetails.newBuilder().apply {
      metricDefinitionBuilder.sketchBuilder.sketchConfigId = 20202
    }.build()

    val REQUISITION: Requisition = Requisition.newBuilder().apply {
      externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
      externalCampaignId = EXTERNAL_CAMPAIGN_ID
      externalRequisitionId = EXTERNAL_REQUISITION_ID
      windowStartTime = START_TIME.toProto()
      windowEndTime = END_TIME.toProto()
      state = RequisitionState.UNFULFILLED
      requisitionDetails = DETAILS
      requisitionDetailsJson = DETAILS.toJson()
    }.build()
  }

  protected fun insertDataProviderMutation(
    dataProviderId: Long = DATA_PROVIDER_ID,
    externalDataProviderId: Long = EXTERNAL_DATA_PROVIDER_ID,
    dataProviderDetails: ByteArray = ByteArray.copyFrom(""),
    dataProviderDetailsJson: String = ""
  ): Mutation =
    Mutation.newInsertBuilder("DataProviders").apply {
      set("DataProviderId").to(dataProviderId)
      set("ExternalDataProviderId").to(externalDataProviderId)
      set("DataProviderDetails").to(dataProviderDetails)
      set("DataProviderDetailsJson").to(dataProviderDetailsJson)
    }.build()

  protected fun insertCampaignMutation(
    dataProviderId: Long = DATA_PROVIDER_ID,
    campaignId: Long = CAMPAIGN_ID,
    advertiserId: Long = IRRELEVANT_ADVERTISER_ID,
    externalCampaignId: Long = EXTERNAL_CAMPAIGN_ID,
    providedCampaignId: String = "irrelevant-provided-campaign-id",
    campaignDetails: ByteArray = ByteArray.copyFrom(""),
    campaignDetailsJson: String = ""
  ): Mutation =
    Mutation.newInsertBuilder("Campaigns").apply {
      set("DataProviderId").to(dataProviderId)
      set("CampaignId").to(campaignId)
      set("AdvertiserId").to(advertiserId)
      set("ExternalCampaignId").to(externalCampaignId)
      set("ProvidedCampaignId").to(providedCampaignId)
      set("CampaignDetails").to(campaignDetails)
      set("CampaignDetailsJson").to(campaignDetailsJson)
    }.build()

  fun insertRequisitionMutation(
    dataProviderId: Long = DATA_PROVIDER_ID,
    campaignId: Long = CAMPAIGN_ID,
    requisitionId: Long = REQUISITION_ID,
    externalRequisitionId: Long = EXTERNAL_REQUISITION_ID,
    createTime: Timestamp = Timestamp.ofTimeMicroseconds(0),
    windowStartTime: Timestamp = START_TIME,
    windowEndTime: Timestamp = END_TIME,
    state: RequisitionState = RequisitionState.UNFULFILLED,
    requisitionDetails: RequisitionDetails = DETAILS
  ): Mutation =
    Mutation.newInsertBuilder("Requisitions").apply {
      set("DataProviderId").to(dataProviderId)
      set("CampaignId").to(campaignId)
      set("RequisitionId").to(requisitionId)
      set("CreateTime").to(createTime)
      set("ExternalRequisitionId").to(externalRequisitionId)
      set("WindowStartTime").to(windowStartTime)
      set("WindowEndTime").to(windowEndTime)
      set("State").toProtoEnum(state)
      set("RequisitionDetails").toProtoBytes(requisitionDetails)
      set("RequisitionDetailsJson").toProtoJson(requisitionDetails)
    }.build()

  fun insertRequisitionMutation(
    campaignId: Long,
    requisitionId: Long,
    requisition: Requisition
  ): Mutation =
    insertRequisitionMutation(
      campaignId = campaignId,
      requisitionId = requisitionId,
      externalRequisitionId = requisition.externalRequisitionId,
      createTime = requisition.createTime.toGcpTimestamp(),
      state = requisition.state
    )
}
