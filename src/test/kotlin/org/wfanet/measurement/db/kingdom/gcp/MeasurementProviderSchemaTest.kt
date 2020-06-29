package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.ByteArray
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Struct
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.db.gcp.testing.UsingSpannerEmulator
import org.wfanet.measurement.db.gcp.testing.assertQueryReturns

@RunWith(JUnit4::class)
class MeasurementProviderSchemaTest :
  UsingSpannerEmulator("/src/main/db/gcp/measurement_provider.sdl") {

  @Test
  fun `insert single Advertiser`() {
    val dbClient = databaseClient
    val mutation = Mutation.newInsertBuilder("Advertisers")
      .set("AdvertiserId").to(3011)
      .set("ExternalAdvertiserId").to(1)
      .set("AdvertiserDetails").to(ByteArray.copyFrom("123"))
      .set("AdvertiserDetailsJSON").to(ByteArray.copyFrom("123"))
      .build()
    dbClient.write(listOf(mutation))
    assertQueryReturns(
      dbClient,
      "SELECT AdvertiserId, ExternalAdvertiserId FROM Advertisers",
      Struct.newBuilder()
        .set("AdvertiserId").to(3011)
        .set("ExternalAdvertiserId").to(1)
        .build()
    )
  }
}
