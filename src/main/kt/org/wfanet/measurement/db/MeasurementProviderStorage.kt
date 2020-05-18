package org.wfanet.measurement.db

import org.wfanet.measurement.common.Pagination

interface MeasurementProviderStorage {
  suspend fun createRequisition(campaignExternalKey: CampaignExternalKey): Requisition

  suspend fun fulfillRequisition(requisitionExternalKey: RequisitionExternalKey): Requisition

  suspend fun listRequisitions(
    campaignExternalKey: CampaignExternalKey,
    states: Set<RequisitionState>,
    pagination: Pagination
  ): List<Requisition>
}
