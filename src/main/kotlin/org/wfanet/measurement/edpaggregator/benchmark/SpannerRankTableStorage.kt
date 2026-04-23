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
import com.google.cloud.spanner.DatabaseClient
import com.google.cloud.spanner.Key
import com.google.cloud.spanner.KeySet
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.TimestampBound
import com.google.cloud.spanner.Value
import java.util.concurrent.TimeUnit
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.RankEntry

class SpannerRankTableStorage(
  private val dbClient: DatabaseClient,
) : RankTableStorage {

  private val rankTableColumns =
    listOf(
      "DataProviderResourceId",
      "ModelRelease",
      "EncryptedFingerprint",
      "PoolId",
      "RankValue",
    )

  private val rankValueColumns =
    listOf(
      "EncryptedFingerprint",
      "RankValue",
    )

  override suspend fun initializePoolCounter(
    dataProvider: String,
    modelRelease: String,
    poolId: String,
    rankedSize: Long,
  ) {
    dbClient.readWriteTransaction().run { txn ->
        val rs =
          txn.executeQuery(
            Statement.newBuilder(
                "SELECT NextRank FROM PoolCounter" +
                  " WHERE DataProviderResourceId = @dp" +
                  " AND ModelRelease = @model" +
                  " AND PoolId = @poolId"
              )
              .bind("dp")
              .to(dataProvider)
              .bind("model")
              .to(modelRelease)
              .bind("poolId")
              .to(poolId)
              .build()
          )

        if (!rs.next()) {
          txn.buffer(
            Mutation.newInsertBuilder("PoolCounter")
              .set("DataProviderResourceId")
              .to(dataProvider)
              .set("ModelRelease")
              .to(modelRelease)
              .set("PoolId")
              .to(poolId)
              .set("NextRank")
              .to(0L)
              .set("RankedSize")
              .to(rankedSize)
              .build()
          )
        }
        rs.close()
        null
      }
  }

  override suspend fun lookupRanks(
    dataProvider: String,
    modelRelease: String,
    fingerprints: List<SpannerByteArray>,
  ): Map<SpannerByteArray, RankEntry> {
    if (fingerprints.isEmpty()) return emptyMap()

    val keySet =
        KeySet.newBuilder()
          .apply {
            for (fp in fingerprints) {
              addKey(Key.of(dataProvider, modelRelease, fp))
            }
          }
          .build()

    val results = mutableMapOf<SpannerByteArray, RankEntry>()
    val readContext = dbClient.singleUse()
    val rs = readContext.read("RankTable", keySet, rankTableColumns)
    try {
      while (rs.next()) {
        val entry =
          RankEntry(
            dataProviderResourceId =
              rs.getString("DataProviderResourceId"),
            modelRelease = rs.getString("ModelRelease"),
            encryptedFingerprint = rs.getBytes("EncryptedFingerprint"),
            poolId = rs.getString("PoolId"),
            rankValue = rs.getLong("RankValue"),
          )
        results[entry.encryptedFingerprint] = entry
      }
    } finally {
      rs.close()
      readContext.close()
    }

    return results
  }

  override suspend fun lookupRankValues(
    dataProvider: String,
    modelRelease: String,
    fingerprints: List<SpannerByteArray>,
  ): Map<SpannerByteArray, Long> {
    if (fingerprints.isEmpty()) return emptyMap()

    val keySet =
        KeySet.newBuilder()
          .apply {
            for (fp in fingerprints) {
              addKey(Key.of(dataProvider, modelRelease, fp))
            }
          }
          .build()

    val results = mutableMapOf<SpannerByteArray, Long>()
    val readContext = dbClient.singleUse()
    val rs = readContext.read("RankTable", keySet, rankValueColumns)
    try {
      while (rs.next()) {
        results[rs.getBytes("EncryptedFingerprint")] = rs.getLong("RankValue")
      }
    } finally {
      rs.close()
      readContext.close()
    }

    return results
  }

  override suspend fun lookupRankValuesStale(
    dataProvider: String,
    modelRelease: String,
    fingerprints: List<SpannerByteArray>,
    maxStalenessSeconds: Long,
  ): Map<SpannerByteArray, Long> {
    if (fingerprints.isEmpty()) return emptyMap()

    val keySet =
        KeySet.newBuilder()
          .apply {
            for (fp in fingerprints) {
              addKey(Key.of(dataProvider, modelRelease, fp))
            }
          }
          .build()

    val timestampBound = if (maxStalenessSeconds > 0) {
      TimestampBound.ofMaxStaleness(maxStalenessSeconds, TimeUnit.SECONDS)
    } else {
      TimestampBound.strong()
    }

    val results = mutableMapOf<SpannerByteArray, Long>()
    val readContext = dbClient.singleUse(timestampBound)
    val rs = readContext.read("RankTable", keySet, rankValueColumns)
    try {
      while (rs.next()) {
        results[rs.getBytes("EncryptedFingerprint")] = rs.getLong("RankValue")
      }
    } finally {
      rs.close()
      readContext.close()
    }

    return results
  }

  override suspend fun lookupKnownFingerprints(
    dataProvider: String,
    modelRelease: String,
    fingerprints: List<SpannerByteArray>,
  ): Set<SpannerByteArray> {
    if (fingerprints.isEmpty()) return emptySet()

    val keySet =
        KeySet.newBuilder()
          .apply {
            for (fp in fingerprints) {
              addKey(Key.of(dataProvider, modelRelease, fp))
            }
          }
          .build()

    val results = mutableSetOf<SpannerByteArray>()
    val readContext = dbClient.singleUse()
    val rs = readContext.read("RankTable", keySet, listOf("EncryptedFingerprint"))
    try {
      while (rs.next()) {
        results.add(rs.getBytes("EncryptedFingerprint"))
      }
    } finally {
      rs.close()
      readContext.close()
    }

    return results
  }

  override suspend fun allocateRanks(
    dataProvider: String,
    modelRelease: String,
    poolId: String,
    count: Int,
  ): Long {
    var startRank = 0L
    dbClient.readWriteTransaction().run { txn ->
        val rs =
          txn.executeQuery(
            Statement.newBuilder(
                "SELECT NextRank FROM PoolCounter" +
                  " WHERE DataProviderResourceId = @dp" +
                  " AND ModelRelease = @model" +
                  " AND PoolId = @poolId"
              )
              .bind("dp")
              .to(dataProvider)
              .bind("model")
              .to(modelRelease)
              .bind("poolId")
              .to(poolId)
              .build()
          )

        check(rs.next()) {
          "PoolCounter not found for pool $poolId" +
            " (dataProvider=$dataProvider, model=$modelRelease)"
        }
        startRank = rs.getLong("NextRank")
        rs.close()

        txn.buffer(
          Mutation.newUpdateBuilder("PoolCounter")
            .set("DataProviderResourceId")
            .to(dataProvider)
            .set("ModelRelease")
            .to(modelRelease)
            .set("PoolId")
            .to(poolId)
            .set("NextRank")
            .to(startRank + count)
            .build()
        )
        null
      }

    return startRank
  }

  override suspend fun writeRanks(entries: List<RankEntry>) {
    val mutations =
      entries.map { entry ->
        Mutation.newInsertBuilder("RankTable")
          .set("DataProviderResourceId")
          .to(entry.dataProviderResourceId)
          .set("ModelRelease")
          .to(entry.modelRelease)
          .set("EncryptedFingerprint")
          .to(entry.encryptedFingerprint)
          .set("PoolId")
          .to(entry.poolId)
          .set("RankValue")
          .to(entry.rankValue)
          .set("CreateTime")
          .to(Value.COMMIT_TIMESTAMP)
          .build()
      }
    dbClient.write(mutations)
  }

  override fun close() {}
}
