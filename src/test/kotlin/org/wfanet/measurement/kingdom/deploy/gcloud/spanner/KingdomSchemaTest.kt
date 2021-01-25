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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner

import com.google.cloud.ByteArray
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Struct
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.gcloud.spanner.testing.UsingSpannerEmulator
import org.wfanet.measurement.gcloud.spanner.testing.assertQueryReturns
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.KINGDOM_SCHEMA

@RunWith(JUnit4::class)
class KingdomSchemaTest : UsingSpannerEmulator(KINGDOM_SCHEMA) {

  @Test
  fun `insert single Advertiser`() = runBlocking {
    val mutation = Mutation.newInsertBuilder("Advertisers")
      .set("AdvertiserId").to(3011)
      .set("ExternalAdvertiserId").to(1)
      .set("AdvertiserDetails").to(ByteArray.copyFrom("123"))
      .set("AdvertiserDetailsJSON").to(ByteArray.copyFrom("123"))
      .build()
    databaseClient.write(listOf(mutation))
    assertQueryReturns(
      databaseClient,
      "SELECT AdvertiserId, ExternalAdvertiserId FROM Advertisers",
      Struct.newBuilder()
        .set("AdvertiserId").to(3011)
        .set("ExternalAdvertiserId").to(1)
        .build()
    )
  }
}
