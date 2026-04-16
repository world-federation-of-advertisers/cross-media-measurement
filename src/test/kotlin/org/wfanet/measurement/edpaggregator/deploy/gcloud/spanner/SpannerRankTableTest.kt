/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner

import com.google.cloud.ByteArray as SpannerByteArray
import com.google.common.truth.Truth.assertThat
import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.PoolCounterEntry
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.RankEntry
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.allocateRanks
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.batchInsertRankEntries
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.bulkLookupRanks
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.insertPoolCounter
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.insertRankEntry
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.readPoolCounter
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule

@RunWith(JUnit4::class)
class SpannerRankTableTest {
  @get:Rule
  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.EDP_AGGREGATOR_CHANGELOG_PATH)

  private val databaseClient
    get() = spannerDatabase.databaseClient

  @Test
  fun `insertRankEntry and bulkLookupRanks returns inserted entries`() = runBlocking {
    val fingerprint1 = SpannerByteArray.copyFrom(byteArrayOf(1, 2, 3, 4, 5, 6, 7, 8))
    val fingerprint2 = SpannerByteArray.copyFrom(byteArrayOf(9, 10, 11, 12, 13, 14, 15, 16))

    val entry1 =
      RankEntry(
        dataProviderResourceId = DATA_PROVIDER,
        modelRelease = MODEL_RELEASE,
        encryptedFingerprint = fingerprint1,
        poolId = "pool-f18",
        rankValue = 0,
      )
    val entry2 =
      RankEntry(
        dataProviderResourceId = DATA_PROVIDER,
        modelRelease = MODEL_RELEASE,
        encryptedFingerprint = fingerprint2,
        poolId = "pool-m18",
        rankValue = 1,
      )

    databaseClient.readWriteTransaction { txn ->
      txn.insertRankEntry(entry1)
      txn.insertRankEntry(entry2)
    }

    var results: Map<SpannerByteArray, RankEntry> = emptyMap()
    databaseClient.readWriteTransaction { txn ->
      results = txn.bulkLookupRanks(DATA_PROVIDER, MODEL_RELEASE, listOf(fingerprint1, fingerprint2))
    }

    assertThat(results).hasSize(2)
    assertThat(results[fingerprint1]?.poolId).isEqualTo("pool-f18")
    assertThat(results[fingerprint1]?.rankValue).isEqualTo(0)
    assertThat(results[fingerprint2]?.poolId).isEqualTo("pool-m18")
    assertThat(results[fingerprint2]?.rankValue).isEqualTo(1)
  }

  @Test
  fun `bulkLookupRanks returns empty map for unknown fingerprints`() = runBlocking {
    val unknownFingerprint = SpannerByteArray.copyFrom(byteArrayOf(99, 98, 97, 96))

    var results: Map<SpannerByteArray, RankEntry> = emptyMap()
    databaseClient.readWriteTransaction { txn ->
      results = txn.bulkLookupRanks(DATA_PROVIDER, MODEL_RELEASE, listOf(unknownFingerprint))
    }

    assertThat(results).isEmpty()
  }

  @Test
  fun `bulkLookupRanks with empty list returns empty map`() = runBlocking {
    var results: Map<SpannerByteArray, RankEntry> = emptyMap()
    databaseClient.readWriteTransaction { txn ->
      results = txn.bulkLookupRanks(DATA_PROVIDER, MODEL_RELEASE, emptyList())
    }

    assertThat(results).isEmpty()
  }

  @Test
  fun `batchInsertRankEntries inserts all entries`() = runBlocking {
    val entries =
      (1..5).map { i ->
        RankEntry(
          dataProviderResourceId = DATA_PROVIDER,
          modelRelease = MODEL_RELEASE,
          encryptedFingerprint = SpannerByteArray.copyFrom(byteArrayOf(i.toByte(), 0, 0, 0)),
          poolId = "pool-$i",
          rankValue = i.toLong(),
        )
      }

    databaseClient.readWriteTransaction { txn -> txn.batchInsertRankEntries(entries) }

    val fingerprints = entries.map { it.encryptedFingerprint }
    var results: Map<SpannerByteArray, RankEntry> = emptyMap()
    databaseClient.readWriteTransaction { txn ->
      results = txn.bulkLookupRanks(DATA_PROVIDER, MODEL_RELEASE, fingerprints)
    }

    assertThat(results).hasSize(5)
  }

  @Test
  fun `insertPoolCounter and readPoolCounter`() = runBlocking {
    val poolCounter =
      PoolCounterEntry(
        dataProviderResourceId = DATA_PROVIDER,
        modelRelease = MODEL_RELEASE,
        poolId = "pool-f18",
        nextRank = 0,
        rankedSize = 5000,
      )

    databaseClient.readWriteTransaction { txn -> txn.insertPoolCounter(poolCounter) }

    var result: PoolCounterEntry? = null
    databaseClient.readWriteTransaction { txn ->
      result = txn.readPoolCounter(DATA_PROVIDER, MODEL_RELEASE, "pool-f18")
    }

    assertThat(result).isNotNull()
    assertThat(result!!.nextRank).isEqualTo(0)
    assertThat(result!!.rankedSize).isEqualTo(5000)
  }

  @Test
  fun `readPoolCounter returns null for unknown pool`() = runBlocking {
    var result: PoolCounterEntry? = null
    databaseClient.readWriteTransaction { txn ->
      result = txn.readPoolCounter(DATA_PROVIDER, MODEL_RELEASE, "nonexistent-pool")
    }

    assertThat(result).isNull()
  }

  @Test
  fun `allocateRanks increments counter and returns starting rank`() = runBlocking {
    val poolCounter =
      PoolCounterEntry(
        dataProviderResourceId = DATA_PROVIDER,
        modelRelease = MODEL_RELEASE,
        poolId = "pool-alloc",
        nextRank = 0,
        rankedSize = 10000,
      )

    databaseClient.readWriteTransaction { txn -> txn.insertPoolCounter(poolCounter) }

    // First allocation: 100 ranks starting from 0
    var startRank1: Long = -1
    databaseClient.readWriteTransaction { txn ->
      startRank1 = txn.allocateRanks(DATA_PROVIDER, MODEL_RELEASE, "pool-alloc", 100)
    }
    assertThat(startRank1).isEqualTo(0)

    // Second allocation: 50 ranks starting from 100
    var startRank2: Long = -1
    databaseClient.readWriteTransaction { txn ->
      startRank2 = txn.allocateRanks(DATA_PROVIDER, MODEL_RELEASE, "pool-alloc", 50)
    }
    assertThat(startRank2).isEqualTo(100)

    // Verify counter is now at 150
    var result: PoolCounterEntry? = null
    databaseClient.readWriteTransaction { txn ->
      result = txn.readPoolCounter(DATA_PROVIDER, MODEL_RELEASE, "pool-alloc")
    }
    assertThat(result!!.nextRank).isEqualTo(150)
  }

  @Test
  fun `bulkLookupRanks returns only entries for matching model release`() = runBlocking {
    val fingerprint = SpannerByteArray.copyFrom(byteArrayOf(42, 43, 44, 45))

    val entry1 =
      RankEntry(
        dataProviderResourceId = DATA_PROVIDER,
        modelRelease = "model-v1",
        encryptedFingerprint = fingerprint,
        poolId = "pool-a",
        rankValue = 10,
      )
    val entry2 =
      RankEntry(
        dataProviderResourceId = DATA_PROVIDER,
        modelRelease = "model-v2",
        encryptedFingerprint = fingerprint,
        poolId = "pool-b",
        rankValue = 20,
      )

    databaseClient.readWriteTransaction { txn ->
      txn.insertRankEntry(entry1)
      txn.insertRankEntry(entry2)
    }

    // Lookup only model-v1
    var results: Map<SpannerByteArray, RankEntry> = emptyMap()
    databaseClient.readWriteTransaction { txn ->
      results = txn.bulkLookupRanks(DATA_PROVIDER, "model-v1", listOf(fingerprint))
    }

    assertThat(results).hasSize(1)
    assertThat(results[fingerprint]?.poolId).isEqualTo("pool-a")
    assertThat(results[fingerprint]?.rankValue).isEqualTo(10)
  }

  companion object {
    private const val DATA_PROVIDER = "dataProviders/dp1"
    private const val MODEL_RELEASE = "model-v1"

    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()
  }
}
