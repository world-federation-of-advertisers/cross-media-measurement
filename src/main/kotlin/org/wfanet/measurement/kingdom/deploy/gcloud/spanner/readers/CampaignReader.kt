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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers

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
