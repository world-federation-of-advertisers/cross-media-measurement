package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.ReadContext
import com.google.cloud.spanner.Struct
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.db.gcp.appendClause
import org.wfanet.measurement.internal.kingdom.Advertiser

class AdvertiserReader : SpannerReader<AdvertiserReadResult>() {
  override val baseSql: String =
    """
    SELECT
      Advertisers.AdvertiserId,
      Advertisers.ExternalAdvertiserId,
      Advertisers.AdvertiserDetails,
      Advertisers.AdvertiserDetailsJson
    FROM Advertisers
    """.trimIndent()

  override suspend fun translate(struct: Struct): AdvertiserReadResult =
    AdvertiserReadResult(buildAdvertiser(struct), struct.getLong("AdvertiserId"))

  private fun buildAdvertiser(struct: Struct): Advertiser = Advertiser.newBuilder().apply {
    externalAdvertiserId = struct.getLong("ExternalAdvertiserId")
  }.build()

  companion object {
    /**
     * Returns an [AdvertiserReadResult] given an external advertiser id, or null if no such id
     * exists.
     */
    suspend fun forExternalId(
      readContext: ReadContext,
      externalAdvertiserId: ExternalId
    ): AdvertiserReadResult? {
      return AdvertiserReader()
        .withBuilder {
          appendClause("WHERE Advertisers.ExternalAdvertiserId = @external_advertiser_id")
          bind("external_advertiser_id").to(externalAdvertiserId.value)
        }
        .execute(readContext)
        .singleOrNull()
    }
  }
}

// TODO: move into AdvertiserReader.
data class AdvertiserReadResult(
  val advertiser: Advertiser,
  val advertiserId: Long
)
