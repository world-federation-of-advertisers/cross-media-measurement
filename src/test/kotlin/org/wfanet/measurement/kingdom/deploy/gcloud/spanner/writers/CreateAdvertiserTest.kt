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

import com.google.cloud.ByteArray
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.TimestampBound
import com.google.common.truth.Truth.assertThat
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.testing.FixedIdGenerator
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.KingdomDatabaseTestBase

@RunWith(JUnit4::class)
class CreateAdvertiserTest : KingdomDatabaseTestBase() {
  @Test
  fun success() = runBlocking<Unit> {
    val idGenerator = FixedIdGenerator()
    CreateAdvertiser().execute(databaseClient, idGenerator)

    val advertisers = databaseClient
      .singleUse(TimestampBound.strong())
      .executeQuery(Statement.of("SELECT * FROM Advertisers"))
      .toList()

    assertThat(advertisers)
      .containsExactly(
        Struct.newBuilder()
          .set("AdvertiserId").to(idGenerator.internalId.value)
          .set("ExternalAdvertiserId").to(idGenerator.externalId.value)
          .set("AdvertiserDetails").to(ByteArray.copyFrom(""))
          .set("AdvertiserDetailsJson").to("")
          .build()
      )
  }
}
