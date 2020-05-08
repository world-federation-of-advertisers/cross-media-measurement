package org.wfanet.measurement.service.db.gcp

import com.google.cloud.ByteArray
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Value
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.service.db.gcp.testing.UsingSpannerEmulator
import kotlin.test.assertEquals

@RunWith(JUnit4::class)
class MeasurementProviderSchemaTest : UsingSpannerEmulator("/src/main/db/gcp/measurement_provider.sdl") {

  @Test
  fun `insert single Advertiser`() {
    val dbClient = spanner.client
    val mutation = Mutation.newInsertBuilder("Advertisers")
      .set("AdvertiserId").to(3011)
      .set("ExternalAdvertiserId").to(1)
      .set("AdvertiserDetails").to(ByteArray.copyFrom("123"))
      .set("AdvertiserDetailsJSON").to(ByteArray.copyFrom("123"))
      .build()
    dbClient.write(listOf(mutation))
    val resultSet = dbClient.singleUse().executeQuery(
      Statement.of("SELECT AdvertiserId, ExternalAdvertiserId FROM Advertisers;"));
    println("Results:")
    while (resultSet.next()) {
      assertEquals(3011, resultSet.getLong(0), "Wrong AdvertiserId")
      assertEquals(1, resultSet.getLong(1), "Wrong ExternalAdvertiserId")
    }
  }
}
