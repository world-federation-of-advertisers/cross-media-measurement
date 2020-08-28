package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.Struct
import org.wfanet.measurement.internal.kingdom.Advertiser

class AdvertiserReader : SpannerReader<AdvertiserReader.Result>() {
  data class Result(
    val advertiser: Advertiser,
    val advertiserId: Long
  )

  override val baseSql: String =
    """
    SELECT
      Advertisers.AdvertiserId,
      Advertisers.ExternalAdvertiserId,
      Advertisers.AdvertiserDetails,
      Advertisers.AdvertiserDetailsJson
    FROM Advertisers
    """.trimIndent()

  override val externalIdColumn: String = "Advertisers.ExternalAdvertiserId"

  override suspend fun translate(struct: Struct): Result =
    Result(buildAdvertiser(struct), struct.getLong("AdvertiserId"))

  private fun buildAdvertiser(struct: Struct): Advertiser = Advertiser.newBuilder().apply {
    externalAdvertiserId = struct.getLong("ExternalAdvertiserId")
  }.build()
}
