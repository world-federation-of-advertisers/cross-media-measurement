package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.Statement

val REQUISITION_READ_QUERY: Statement = Statement.of(
  """
  SELECT Requisitions.DataProviderId,
         Requisitions.CampaignId,
         Requisitions.RequisitionId,
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
)
