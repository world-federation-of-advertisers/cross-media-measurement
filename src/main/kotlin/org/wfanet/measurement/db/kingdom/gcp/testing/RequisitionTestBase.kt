package org.wfanet.measurement.db.kingdom.gcp.testing

import com.google.cloud.Timestamp
import org.wfanet.measurement.common.toJson
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
}
