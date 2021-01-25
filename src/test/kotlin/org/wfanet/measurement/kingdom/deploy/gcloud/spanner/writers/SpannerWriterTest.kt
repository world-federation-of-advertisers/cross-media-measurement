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
import com.google.cloud.Timestamp
import com.google.cloud.spanner.Key
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.TimestampBound
import com.google.common.truth.Truth.assertThat
import java.time.Clock
import kotlin.test.assertFails
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.identity.testing.FixedIdGenerator
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.KingdomDatabaseTestBase

@RunWith(JUnit4::class)
class SpannerWriterTest : KingdomDatabaseTestBase() {
  private val idGenerator = FixedIdGenerator()

  @Test
  fun `Transaction with no writes succeeds`() = runBlocking<Unit> {
    val transaction = object : SpannerWriter<Long, String>() {
      override suspend fun TransactionScope.runTransaction(): Long {
        return idGenerator.generateExternalId().value
      }

      override fun ResultScope<Long>.buildResult(): String {
        return requireNotNull(transactionResult).toString()
      }
    }
    val result = transaction.execute(databaseClient, idGenerator, Clock.systemUTC())
    assertThat(result).isEqualTo(idGenerator.externalId.value.toString())
  }

  @Test
  fun `Transaction with writes succeeds`() = runBlocking<Unit> {
    val transaction = object : SpannerWriter<InternalId, Timestamp>() {
      override suspend fun TransactionScope.runTransaction(): InternalId {
        val internalId = idGenerator.generateInternalId()
        transactionContext.buffer(
          Mutation.newInsertBuilder("Advertisers")
            .set("AdvertiserId").to(internalId.value)
            .set("ExternalAdvertiserId").to(idGenerator.generateExternalId().value)
            .set("AdvertiserDetails").to(ByteArray.copyFrom(""))
            .set("AdvertiserDetailsJson").to("irrelevant-advertiser-details-json")
            .build()
        )
        return internalId
      }

      override fun ResultScope<InternalId>.buildResult(): Timestamp {
        return commitTimestamp
      }
    }

    val commitTimestamp = transaction.execute(databaseClient, idGenerator, Clock.systemUTC())

    // Verify the transaction committed.
    val bound = TimestampBound.ofReadTimestamp(commitTimestamp)
    val readContext = databaseClient.singleUse(bound)
    val key = Key.of(idGenerator.internalId.value)
    val row = readContext.readRow("Advertisers", key, listOf("ExternalAdvertiserId"))
    assertThat(requireNotNull(row).getLong(0))
      .isEqualTo(idGenerator.externalId.value)
  }

  @Test
  fun `multiple execution fails`() = runBlocking<Unit> {
    val transaction = object : SpannerWriter<Long, String>() {
      override suspend fun TransactionScope.runTransaction(): Long {
        return 1
      }

      override fun ResultScope<Long>.buildResult(): String {
        return "the-result"
      }
    }

    // First execution should succeed
    val result = transaction.execute(databaseClient, idGenerator, Clock.systemUTC())
    assertThat(result).isEqualTo("the-result")

    // Second execution should fail
    assertFails {
      transaction.execute(databaseClient, idGenerator, Clock.systemUTC())
    }
  }
}
