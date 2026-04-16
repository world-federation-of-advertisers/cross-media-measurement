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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db

import com.google.cloud.ByteArray as SpannerByteArray
import com.google.cloud.spanner.Options
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import org.wfanet.measurement.common.singleOrNullIfEmpty
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.statement

/** Rank entry from the RankTable. */
data class RankEntry(
  val dataProviderResourceId: String,
  val modelRelease: String,
  val encryptedFingerprint: SpannerByteArray,
  val poolId: String,
  val rankValue: Long,
)

/** Pool counter from the PoolCounter table. */
data class PoolCounterEntry(
  val dataProviderResourceId: String,
  val modelRelease: String,
  val poolId: String,
  val nextRank: Long,
  val rankedSize: Long,
)

/**
 * Bulk lookup of rank entries by encrypted fingerprints.
 *
 * @return a map of encrypted fingerprint to [RankEntry] for found entries
 */
suspend fun AsyncDatabaseClient.ReadContext.bulkLookupRanks(
  dataProviderResourceId: String,
  modelRelease: String,
  encryptedFingerprints: List<SpannerByteArray>,
): Map<SpannerByteArray, RankEntry> {
  if (encryptedFingerprints.isEmpty()) return emptyMap()

  val sql =
    """
    SELECT
      DataProviderResourceId,
      ModelRelease,
      EncryptedFingerprint,
      PoolId,
      RankValue,
    FROM
      RankTable
    WHERE
      DataProviderResourceId = @dataProviderResourceId
      AND ModelRelease = @modelRelease
      AND EncryptedFingerprint IN UNNEST(@encryptedFingerprints)
    """
      .trimIndent()

  val query =
    statement(sql) {
      bind("dataProviderResourceId").to(dataProviderResourceId)
      bind("modelRelease").to(modelRelease)
      bind("encryptedFingerprints").toBytesArray(encryptedFingerprints)
    }

  return buildMap {
    executeQuery(query, Options.tag("action=bulkLookupRanks")).collect { row ->
      val entry = buildRankEntry(row)
      put(entry.encryptedFingerprint, entry)
    }
  }
}

/** Buffers an insert mutation for a [RankEntry] row. */
fun AsyncDatabaseClient.TransactionContext.insertRankEntry(entry: RankEntry) {
  bufferInsertMutation("RankTable") {
    set("DataProviderResourceId").to(entry.dataProviderResourceId)
    set("ModelRelease").to(entry.modelRelease)
    set("EncryptedFingerprint").to(entry.encryptedFingerprint)
    set("PoolId").to(entry.poolId)
    set("RankValue").to(entry.rankValue)
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Buffers insert mutations for multiple [RankEntry] rows. */
fun AsyncDatabaseClient.TransactionContext.batchInsertRankEntries(entries: List<RankEntry>) {
  for (entry in entries) {
    insertRankEntry(entry)
  }
}

/**
 * Reads the [PoolCounterEntry] for a specific pool.
 *
 * @return the [PoolCounterEntry], or null if not found
 */
suspend fun AsyncDatabaseClient.ReadContext.readPoolCounter(
  dataProviderResourceId: String,
  modelRelease: String,
  poolId: String,
): PoolCounterEntry? {
  val sql =
    """
    SELECT
      DataProviderResourceId,
      ModelRelease,
      PoolId,
      NextRank,
      RankedSize,
    FROM
      PoolCounter
    WHERE
      DataProviderResourceId = @dataProviderResourceId
      AND ModelRelease = @modelRelease
      AND PoolId = @poolId
    """
      .trimIndent()

  val row: Struct =
    executeQuery(
        statement(sql) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("modelRelease").to(modelRelease)
          bind("poolId").to(poolId)
        },
        Options.tag("action=readPoolCounter"),
      )
      .singleOrNullIfEmpty() ?: return null

  return buildPoolCounterEntry(row)
}

/**
 * Atomically allocates [count] ranks from the pool counter.
 *
 * Returns the starting rank value. The allocated range is [startRank, startRank + count).
 *
 * Must be called within a read-write transaction.
 *
 * @throws IllegalStateException if the PoolCounter row does not exist
 */
suspend fun AsyncDatabaseClient.TransactionContext.allocateRanks(
  dataProviderResourceId: String,
  modelRelease: String,
  poolId: String,
  count: Int,
): Long {
  val sql =
    """
    SELECT NextRank
    FROM PoolCounter
    WHERE
      DataProviderResourceId = @dataProviderResourceId
      AND ModelRelease = @modelRelease
      AND PoolId = @poolId
    """
      .trimIndent()

  val row: Struct =
    executeQuery(
        statement(sql) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("modelRelease").to(modelRelease)
          bind("poolId").to(poolId)
        }
      )
      .singleOrNullIfEmpty()
      ?: error(
        "PoolCounter not found for pool $poolId " +
          "(dataProvider=$dataProviderResourceId, model=$modelRelease)"
      )

  val startRank = row.getLong("NextRank")

  bufferUpdateMutation("PoolCounter") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("ModelRelease").to(modelRelease)
    set("PoolId").to(poolId)
    set("NextRank").to(startRank + count)
  }

  return startRank
}

/** Buffers an insert mutation for a [PoolCounterEntry] row. */
fun AsyncDatabaseClient.TransactionContext.insertPoolCounter(entry: PoolCounterEntry) {
  bufferInsertMutation("PoolCounter") {
    set("DataProviderResourceId").to(entry.dataProviderResourceId)
    set("ModelRelease").to(entry.modelRelease)
    set("PoolId").to(entry.poolId)
    set("NextRank").to(entry.nextRank)
    set("RankedSize").to(entry.rankedSize)
  }
}

private fun buildRankEntry(struct: Struct): RankEntry {
  return RankEntry(
    dataProviderResourceId = struct.getString("DataProviderResourceId"),
    modelRelease = struct.getString("ModelRelease"),
    encryptedFingerprint = struct.getBytes("EncryptedFingerprint"),
    poolId = struct.getString("PoolId"),
    rankValue = struct.getLong("RankValue"),
  )
}

private fun buildPoolCounterEntry(struct: Struct): PoolCounterEntry {
  return PoolCounterEntry(
    dataProviderResourceId = struct.getString("DataProviderResourceId"),
    modelRelease = struct.getString("ModelRelease"),
    poolId = struct.getString("PoolId"),
    nextRank = struct.getLong("NextRank"),
    rankedSize = struct.getLong("RankedSize"),
  )
}
