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

package org.wfanet.measurement.edpaggregator.benchmark

import com.google.cloud.ByteArray as SpannerByteArray
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.PoolCounterEntry
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.RankEntry
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.allocateRanks
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.batchInsertRankEntries
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.bulkLookupRanks
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.insertPoolCounter
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.readPoolCounter
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.virtualpeople.core.labeler.Labeler

data class DayResult(
  val bulkLookupMs: Long,
  val lookupCount: Int,
  val pass1Ms: Long,
  val pass1Count: Int,
  val rankAllocMs: Long,
  val rankAllocPoolCount: Int,
  val writeRanksMs: Long,
  val writeCount: Int,
  val pass2Ms: Long,
  val pass2Count: Int,
) {
  val totalMs: Long
    get() = bulkLookupMs + pass1Ms + rankAllocMs + writeRanksMs + pass2Ms
}

class Pipeline(
  private val databaseClient: AsyncDatabaseClient,
  private val labeler: Labeler,
  private val dataProvider: String,
  private val modelRelease: String,
  private val numPools: Int,
  private val dbBatchSize: Int,
) {
  /** Initialize pool counters if they don't exist yet. */
  suspend fun initializePoolCounters() {
    for (i in 0 until numPools) {
      val poolId = "pool-$i"
      databaseClient.readWriteTransaction().run { txn ->
        val existing = txn.readPoolCounter(dataProvider, modelRelease, poolId)
        if (existing == null) {
          txn.insertPoolCounter(
            PoolCounterEntry(
              dataProviderResourceId = dataProvider,
              modelRelease = modelRelease,
              poolId = poolId,
              nextRank = 0,
              rankedSize = 10_000_000,
            )
          )
        }
      }
    }
    println("Pool counters initialized ($numPools pools).")
  }

  /** Run the full memoized VID pipeline for one day. */
  suspend fun runDay(dayData: DayData): DayResult {
    val allAccountIds = dayData.newAccountIds + dayData.returningAccountIds

    // Map userId -> (encrypted fingerprint, account index for labeler input)
    val accountFingerprints: Map<String, SpannerByteArray> =
      allAccountIds.associateWith { DataGenerator.encryptFingerprint(it) }

    // ===== Phase 1: Bulk Lookup =====
    val knownRanks = mutableMapOf<SpannerByteArray, RankEntry>()
    val bulkLookupMs = timed {
      val allFingerprints = accountFingerprints.values.toList()
      for (batch in allFingerprints.chunked(dbBatchSize)) {
        databaseClient.readWriteTransaction().run { txn ->
          val results = txn.bulkLookupRanks(dataProvider, modelRelease, batch)
          knownRanks.putAll(results)
        }
      }
    }

    // Determine which accounts are new (not in rank table)
    val newAccountIds =
      allAccountIds.filter { userId ->
        val fp = accountFingerprints[userId]!!
        !knownRanks.containsKey(fp)
      }

    // ===== Phase 2: Pass 1 — Labeler for new accounts =====
    // Run labeler to determine pool assignment. Pool is derived from VID.
    val newAccountPoolAssignments = mutableMapOf<String, String>() // userId -> poolId
    val pass1Ms = timed {
      for (userId in newAccountIds) {
        val accountIndex = userId.removePrefix("user_").toInt()
        val input = DataGenerator.buildLabelerInput(userId, accountIndex)
        val output = labeler.label(input)
        // Derive pool_id from the VID output
        val vid =
          if (output.peopleList.isNotEmpty()) {
            output.peopleList[0].virtualPersonId
          } else {
            0L
          }
        val poolId = "pool-${Math.abs(vid.hashCode()) % numPools}"
        newAccountPoolAssignments[userId] = poolId
      }
    }

    // ===== Phase 3: Allocate Ranks =====
    val poolToStartRank = mutableMapOf<String, Long>()
    val poolGroups = newAccountPoolAssignments.entries.groupBy({ it.value }, { it.key })
    val rankAllocMs = timed {
      for ((poolId, userIds) in poolGroups) {
        databaseClient.readWriteTransaction().run { txn ->
          poolToStartRank[poolId] = txn.allocateRanks(
            dataProvider, modelRelease, poolId, userIds.size
          )
        }
      }
    }

    // ===== Phase 4: Write New Ranks =====
    val newRankEntries = mutableListOf<RankEntry>()
    for ((poolId, userIds) in poolGroups) {
      val startRank = poolToStartRank[poolId]!!
      userIds.forEachIndexed { index, userId ->
        newRankEntries.add(
          RankEntry(
            dataProviderResourceId = dataProvider,
            modelRelease = modelRelease,
            encryptedFingerprint = accountFingerprints[userId]!!,
            poolId = poolId,
            rankValue = startRank + index,
          )
        )
      }
    }
    val writeRanksMs = timed {
      for (batch in newRankEntries.chunked(dbBatchSize)) {
        databaseClient.readWriteTransaction().run { txn ->
          txn.batchInsertRankEntries(batch)
        }
      }
    }

    // ===== Phase 5: Pass 2 — Labeler for all impressions =====
    val impressionCount = dayData.totalImpressions
    val pass2Ms = timed {
      // Distribute impressions across all accounts round-robin
      for (i in 0 until impressionCount) {
        val userId = allAccountIds[i % allAccountIds.size]
        val accountIndex = userId.removePrefix("user_").toInt()
        val input = DataGenerator.buildLabelerInput(userId, accountIndex)
        labeler.label(input)
      }
    }

    return DayResult(
      bulkLookupMs = bulkLookupMs,
      lookupCount = allAccountIds.size,
      pass1Ms = pass1Ms,
      pass1Count = newAccountIds.size,
      rankAllocMs = rankAllocMs,
      rankAllocPoolCount = poolGroups.size,
      writeRanksMs = writeRanksMs,
      writeCount = newRankEntries.size,
      pass2Ms = pass2Ms,
      pass2Count = impressionCount,
    )
  }
}

/** Measures wall-clock time of a suspend block in milliseconds. */
private suspend inline fun timed(block: suspend () -> Unit): Long {
  val startNs = System.nanoTime()
  block()
  return (System.nanoTime() - startNs) / 1_000_000
}
