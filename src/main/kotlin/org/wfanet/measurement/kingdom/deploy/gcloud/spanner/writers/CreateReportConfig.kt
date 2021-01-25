// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.cloud.spanner.Mutation
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.onEach
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bufferTo
import org.wfanet.measurement.gcloud.spanner.toProtoBytes
import org.wfanet.measurement.gcloud.spanner.toProtoEnum
import org.wfanet.measurement.gcloud.spanner.toProtoJson
import org.wfanet.measurement.internal.kingdom.ReportConfig
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.AdvertiserReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.CampaignReader

/**
 * Creates ReportConfigs.
 */
class CreateReportConfig(
  private val reportConfig: ReportConfig,
  private val campaigns: List<ExternalId>
) : SimpleSpannerWriter<ReportConfig>() {
  override suspend fun TransactionScope.runTransaction(): ReportConfig {
    val advertiserId =
      AdvertiserReader()
        .readExternalId(transactionContext, ExternalId(reportConfig.externalAdvertiserId))
        .advertiserId

    val actualReportConfig = reportConfig.toBuilder().apply {
      externalReportConfigId = idGenerator.generateExternalId().value
      numRequisitions = (campaigns.size * reportConfigDetails.metricDefinitionsCount).toLong()
      state = ReportConfig.ReportConfigState.ACTIVE
      reportConfigDetailsJson = reportConfigDetails.toJson()
    }.build()

    val reportConfigId = idGenerator.generateInternalId().value
    insertReportConfig(actualReportConfig, advertiserId, reportConfigId)

    readCampaigns()
      .onEach {
        require(it.advertiserId == advertiserId) {
          "Campaign has unexpected advertiser ${it.campaign.externalAdvertiserId} instead of " +
            "the ReportConfig's advertiser: ${reportConfig.externalAdvertiserId}"
        }
      }
      .collect { addCampaignToReportConfig(reportConfigId, it) }

    return actualReportConfig
  }

  private fun TransactionScope.readCampaigns(): Flow<CampaignReader.Result> {
    return CampaignReader()
      .withBuilder {
        appendClause("WHERE Campaigns.ExternalCampaignId IN UNNEST(@external_campaign_ids)")
        bind("external_campaign_ids").toInt64Array(campaigns.map { it.value })
      }
      .execute(transactionContext)
  }

  private fun TransactionScope.insertReportConfig(
    reportConfig: ReportConfig,
    advertiserId: Long,
    reportConfigId: Long
  ) {
    Mutation.newInsertBuilder("ReportConfigs")
      .set("AdvertiserId").to(advertiserId)
      .set("ReportConfigId").to(reportConfigId)
      .set("ExternalReportConfigId").to(reportConfig.externalReportConfigId)
      .set("NumRequisitions").to(reportConfig.numRequisitions)
      .set("State").toProtoEnum(reportConfig.state)
      .set("ReportConfigDetails").toProtoBytes(reportConfig.reportConfigDetails)
      .set("ReportConfigDetailsJson").toProtoJson(reportConfig.reportConfigDetails)
      .build()
      .bufferTo(transactionContext)
  }

  private fun TransactionScope.addCampaignToReportConfig(
    reportConfigId: Long,
    campaignReadResult: CampaignReader.Result
  ) {
    Mutation.newInsertBuilder("ReportConfigCampaigns")
      .set("AdvertiserId").to(campaignReadResult.advertiserId)
      .set("ReportConfigId").to(reportConfigId)
      .set("DataProviderId").to(campaignReadResult.dataProviderId)
      .set("CampaignId").to(campaignReadResult.campaignId)
      .build()
      .bufferTo(transactionContext)
  }
}
