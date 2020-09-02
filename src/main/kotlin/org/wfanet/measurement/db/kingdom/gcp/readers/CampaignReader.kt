package org.wfanet.measurement.db.kingdom.gcp.readers

import com.google.cloud.spanner.Struct
import org.wfanet.measurement.internal.kingdom.Campaign

class CampaignReader : SpannerReader<CampaignReader.Result>() {
  data class Result(
    val campaign: Campaign,
    val dataProviderId: Long,
    val campaignId: Long,
    val advertiserId: Long
  )

  override val baseSql: String =
    """
    SELECT
      DataProviders.ExternalDataProviderId,
      Advertisers.ExternalAdvertiserId,
      Campaigns.DataProviderId,
      Campaigns.CampaignId,
      Campaigns.ExternalCampaignId,
      Campaigns.ProvidedCampaignId,
      Campaigns.AdvertiserId
    FROM Campaigns
    JOIN DataProviders USING (DataProviderId)
    JOIN Advertisers USING (AdvertiserId)
    """.trimIndent()

  override val externalIdColumn: String = "Campaigns.ExternalCampaignId"

  override suspend fun translate(struct: Struct): Result =
    Result(
      buildCampaign(struct),
      struct.getLong("DataProviderId"),
      struct.getLong("CampaignId"),
      struct.getLong("AdvertiserId")
    )

  private fun buildCampaign(struct: Struct): Campaign = Campaign.newBuilder().apply {
    externalDataProviderId = struct.getLong("ExternalDataProviderId")
    externalCampaignId = struct.getLong("ExternalCampaignId")
    providedCampaignId = struct.getString("ProvidedCampaignId")
    externalAdvertiserId = struct.getLong("ExternalAdvertiserId")
  }.build()
}
