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

package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.ByteArray
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Struct
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.db.gcp.testing.UsingSpannerEmulator
import org.wfanet.measurement.db.gcp.testing.assertQueryReturns

@RunWith(JUnit4::class)
class KingdomSchemaTest : UsingSpannerEmulator("/src/main/db/gcp/kingdom.sdl") {

  @Test
  fun `insert single Advertiser`() = runBlocking {
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
