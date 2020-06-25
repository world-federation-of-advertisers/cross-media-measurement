package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.ReadContext
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.db.gcp.getProtoMessage
import org.wfanet.measurement.db.gcp.single
import org.wfanet.measurement.internal.MetricDefinition
import org.wfanet.measurement.internal.kingdom.ReportConfigDetails
import org.wfanet.measurement.internal.kingdom.RequisitionDetails
import org.wfanet.measurement.internal.kingdom.RequisitionTemplate

class ReadRequisitionTemplatesQuery {
  fun execute(
    readContext: ReadContext,
    externalReportConfigId: ExternalId
  ): Iterable<RequisitionTemplate> {
    val sql =
      """
      SELECT ReportConfigs.ReportConfigDetails,
             ARRAY_AGG(DataProviders.ExternalDataProviderId) AS ExternalDataProviderIds,
             ARRAY_AGG(Campaigns.ExternalCampaignId) AS ExternalCampaignIds,
      FROM ReportConfigs
      JOIN ReportConfigCampaigns USING (AdvertiserId, ReportConfigId)
      JOIN Campaigns USING (DataProviderId, CampaignId)
      JOIN DataProviders USING (DataProviderId)
      WHERE ReportConfigs.ExternalReportConfigId = @external_report_config_id
      GROUP BY 1
      """.trimIndent()

    val statement: Statement =
      Statement.newBuilder(sql)
        .bind("external_report_config_id").to(externalReportConfigId.value)
        .build()

    val struct = readContext.executeQuery(statement).single()
    return buildRequisitionTemplates(struct).asIterable()
  }

  private fun buildRequisitionTemplates(struct: Struct): Sequence<RequisitionTemplate> {
    val reportConfigDetails =
      struct.getProtoMessage("ReportConfigDetails", ReportConfigDetails.parser())

    val dataProviders = struct.getLongList("ExternalDataProviderIds")
    val campaigns = struct.getLongList("ExternalCampaignIds")

    return sequence {
      for ((externalDataProviderId, externalCampaignId) in dataProviders zip campaigns) {
        for (metricDefinition in reportConfigDetails.metricDefinitionsList) {
          yield(
            buildRequisitionTemplate(externalDataProviderId, externalCampaignId, metricDefinition)
          )
        }
      }
    }
  }

  private fun buildRequisitionTemplate(
    externalDataProviderId: Long,
    externalCampaignId: Long,
    metricDefinition: MetricDefinition
  ): RequisitionTemplate =
    RequisitionTemplate.newBuilder()
      .setExternalDataProviderId(externalDataProviderId)
      .setExternalCampaignId(externalCampaignId)
      .setRequisitionDetails(buildRequisitionDetails(metricDefinition))
      .build()

  private fun buildRequisitionDetails(metricDefinition: MetricDefinition): RequisitionDetails =
    RequisitionDetails.newBuilder()
      .setMetricDefinition(metricDefinition)
      .build()
}
