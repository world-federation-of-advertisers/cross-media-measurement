package org.wfanet.measurement.db.kingdom

import java.time.Instant
import org.wfanet.measurement.common.Pagination
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionDetails
import org.wfanet.measurement.internal.kingdom.RequisitionState

interface MeasurementProviderStorage {
  suspend fun createRequisition(
    campaignExternalKey: CampaignExternalKey,
    requisitionDetails: RequisitionDetails,
    windowStartTime: Instant,
    windowEndTime: Instant
  ): Requisition

  suspend fun fulfillRequisition(requisitionExternalKey: RequisitionExternalKey): Requisition

  suspend fun listRequisitions(
    campaignExternalKey: CampaignExternalKey,
    states: Set<RequisitionState>,
    pagination: Pagination
  ): List<Requisition>
}
