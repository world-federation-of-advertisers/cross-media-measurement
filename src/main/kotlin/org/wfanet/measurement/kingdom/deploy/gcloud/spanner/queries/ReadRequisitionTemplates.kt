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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries

import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.flow
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.spanner.getProtoMessage
import org.wfanet.measurement.internal.MetricDefinition
import org.wfanet.measurement.internal.kingdom.ReportConfigDetails
import org.wfanet.measurement.internal.kingdom.RequisitionDetails
import org.wfanet.measurement.internal.kingdom.RequisitionTemplate
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.BaseSpannerReader

@OptIn(FlowPreview::class) // For `flatMapConcat`
class ReadRequisitionTemplates(
  externalReportConfigId: ExternalId
) : SpannerQuery<Struct, RequisitionTemplate>() {
  override val reader: BaseSpannerReader<Struct> by lazy {
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
      LIMIT 1
      """.trimIndent()

    val statement: Statement =
      Statement.newBuilder(sql)
        .bind("external_report_config_id").to(externalReportConfigId.value)
        .build()

    BaseSpannerReader.forStructs(statement)
  }

  override fun Flow<Struct>.transform(): Flow<RequisitionTemplate> {
    return flatMapConcat { buildRequisitionTemplates(it) }
  }

  private fun buildRequisitionTemplates(struct: Struct): Flow<RequisitionTemplate> {
    val reportConfigDetails =
      struct.getProtoMessage("ReportConfigDetails", ReportConfigDetails.parser())

    val dataProviders = struct.getLongList("ExternalDataProviderIds")
    val campaigns = struct.getLongList("ExternalCampaignIds")

    return flow {
      for ((externalDataProviderId, externalCampaignId) in dataProviders zip campaigns) {
        for (metricDefinition in reportConfigDetails.metricDefinitionsList) {
          emit(
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
