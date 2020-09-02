package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.TransactionContext
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.db.gcp.appendClause
import org.wfanet.measurement.db.gcp.spannerDispatcher
import org.wfanet.measurement.db.gcp.toProtoBytes
import org.wfanet.measurement.db.gcp.toProtoEnum
import org.wfanet.measurement.db.gcp.toProtoJson
import org.wfanet.measurement.db.kingdom.gcp.readers.AdvertiserReader
import org.wfanet.measurement.db.kingdom.gcp.readers.CampaignReader
import org.wfanet.measurement.internal.kingdom.ReportConfig

/**
 * Creates ReportConfigs.
 */
class CreateReportConfigTransaction(private val idGenerator: IdGenerator) {
  /**
   * Runs a Spanner transaction to create a ReportConfig.
   *
   * TODO: if it turns out that there are too many campaigns to insert a row for each
   * transactionally, consider making the list of campaigns part of the ReportConfigDetails or
   * adding separate APIs to manage those.
   */
  fun execute(
    transactionContext: TransactionContext,
    reportConfig: ReportConfig,
    campaigns: List<ExternalId>
  ): ReportConfig = runBlocking(spannerDispatcher()) {
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
    transactionContext.buffer(actualReportConfig.toInsertMutation(advertiserId, reportConfigId))

    CampaignReader()
      .withBuilder {
        appendClause("WHERE Campaigns.ExternalCampaignId IN UNNEST(@external_campaign_ids)")
        bind("external_campaign_ids").toInt64Array(campaigns.map { it.value })
      }
      .execute(transactionContext)
      .onEach {
        require(it.advertiserId == advertiserId) {
          "Campaign has unexpected advertiser ${it.campaign.externalAdvertiserId} instead of " +
            "the ReportConfig's advertiser: ${reportConfig.externalAdvertiserId}"
        }
      }
      .map { insertCampaignMutation(reportConfigId, it) }
      .toList()
      .apply(transactionContext::buffer)

    actualReportConfig
  }

  private fun ReportConfig.toInsertMutation(advertiserId: Long, reportConfigId: Long): Mutation {
    return Mutation.newInsertBuilder("ReportConfigs")
      .set("AdvertiserId").to(advertiserId)
      .set("ReportConfigId").to(reportConfigId)
      .set("ExternalReportConfigId").to(externalReportConfigId)
      .set("NumRequisitions").to(numRequisitions)
      .set("State").toProtoEnum(state)
      .set("ReportConfigDetails").toProtoBytes(reportConfigDetails)
      .set("ReportConfigDetailsJson").toProtoJson(reportConfigDetails)
      .build()
  }

  private fun insertCampaignMutation(
    reportConfigId: Long,
    campaignReadResult: CampaignReader.Result
  ): Mutation {
    return Mutation.newInsertBuilder("ReportConfigCampaigns")
      .set("AdvertiserId").to(campaignReadResult.advertiserId)
      .set("ReportConfigId").to(reportConfigId)
      .set("DataProviderId").to(campaignReadResult.dataProviderId)
      .set("CampaignId").to(campaignReadResult.campaignId)
      .build()
  }
}
