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
import com.google.cloud.bigtable.data.v2.BigtableDataClient
import com.google.cloud.bigtable.data.v2.models.BulkMutation
import com.google.cloud.bigtable.data.v2.models.Mutation as BtMutation
import com.google.cloud.bigtable.data.v2.models.Query
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow
import com.google.cloud.bigtable.data.v2.models.RowMutation
import com.google.protobuf.ByteString
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.RankEntry

class BigtableRankTableStorage(
  private val client: BigtableDataClient,
  private val rankTableId: String = "rank-table",
  private val counterTableId: String = "pool-counter",
) : RankTableStorage {

  override suspend fun initializePoolCounter(
    dataProvider: String,
    modelRelease: String,
    poolId: String,
    rankedSize: Long,
  ) =
    withContext(Dispatchers.IO) {
      val rowKey = counterKey(dataProvider, modelRelease, poolId)
      val existing = client.readRow(counterTableId, rowKey)
      if (existing == null) {
        client.mutateRow(
          RowMutation.create(counterTableId, rowKey)
            .setCell(CF, COL_NEXT_RANK, longToBytes(0))
            .setCell(CF, COL_RANKED_SIZE, longToBytes(rankedSize))
        )
      }
    }

  override suspend fun lookupRanks(
    dataProvider: String,
    modelRelease: String,
    fingerprints: List<SpannerByteArray>,
  ): Map<SpannerByteArray, RankEntry> =
    withContext(Dispatchers.IO) {
      if (fingerprints.isEmpty()) return@withContext emptyMap()

      val keyToFp = mutableMapOf<ByteString, SpannerByteArray>()
      val query = Query.create(rankTableId)
      for (fp in fingerprints) {
        val rowKey = rankKey(dataProvider, modelRelease, fp)
        query.rowKey(rowKey)
        keyToFp[rowKey] = fp
      }

      val results = mutableMapOf<SpannerByteArray, RankEntry>()
      for (row in client.readRows(query)) {
        val fp = keyToFp[row.key] ?: continue
        val poolId =
          row.getCells(CF, "pool_id").firstOrNull()?.value?.toStringUtf8()
            ?: continue
        val rankValue =
          row.getCells(CF, COL_RANK_VALUE).firstOrNull()?.value?.let {
            bytesToLong(it)
          } ?: continue
        results[fp] =
          RankEntry(
            dataProviderResourceId = dataProvider,
            modelRelease = modelRelease,
            encryptedFingerprint = fp,
            poolId = poolId,
            rankValue = rankValue,
          )
      }
      results
    }


  override suspend fun lookupKnownFingerprints(
    dataProvider: String,
    modelRelease: String,
    fingerprints: List<SpannerByteArray>,
  ): Set<SpannerByteArray> =
    withContext(Dispatchers.IO) {
      if (fingerprints.isEmpty()) return@withContext emptySet()

      val keyToFp = mutableMapOf<ByteString, SpannerByteArray>()
      val query = Query.create(rankTableId)
      for (fp in fingerprints) {
        val rowKey = rankKey(dataProvider, modelRelease, fp)
        query.rowKey(rowKey)
        keyToFp[rowKey] = fp
      }

      val results = mutableSetOf<SpannerByteArray>()
      for (row in client.readRows(query)) {
        val fp = keyToFp[row.key] ?: continue
        results.add(fp)
      }
      results
    }

  override suspend fun allocateRanks(
    dataProvider: String,
    modelRelease: String,
    poolId: String,
    count: Int,
  ): Long =
    withContext(Dispatchers.IO) {
      val rowKey = counterKey(dataProvider, modelRelease, poolId)
      val result =
        client.readModifyWriteRow(
          ReadModifyWriteRow.create(counterTableId, rowKey)
            .increment(CF, COL_NEXT_RANK, count.toLong())
        )
      val newNextRank =
        bytesToLong(result.getCells(CF, COL_NEXT_RANK).first().value)
      newNextRank - count
    }

  override suspend fun writeRanks(entries: List<RankEntry>) =
    withContext(Dispatchers.IO) {
      val bulk = BulkMutation.create(rankTableId)
      for (entry in entries) {
        val rowKey =
          rankKey(
            entry.dataProviderResourceId,
            entry.modelRelease,
            entry.encryptedFingerprint,
          )
        bulk.add(
          rowKey,
          BtMutation.create()
            .setCell(CF, "pool_id", entry.poolId)
            .setCell(CF, COL_RANK_VALUE, longToBytes(entry.rankValue)),
        )
      }
      client.bulkMutateRows(bulk)
    }

  override fun close() {
    client.close()
  }

  companion object {
    private const val CF = "d"
    private val COL_NEXT_RANK: ByteString = ByteString.copyFromUtf8("next_rank")
    private val COL_RANKED_SIZE: ByteString = ByteString.copyFromUtf8("ranked_size")
    private val COL_RANK_VALUE: ByteString = ByteString.copyFromUtf8("rank_value")

    private fun rankKey(
      dp: String,
      model: String,
      fp: SpannerByteArray,
    ): ByteString {
      return ByteString.copyFromUtf8("$dp#$model#")
        .concat(ByteString.copyFrom(fp.toByteArray()))
    }

    private fun counterKey(
      dp: String,
      model: String,
      poolId: String,
    ): ByteString {
      return ByteString.copyFromUtf8("$dp#$model#$poolId")
    }

    private fun longToBytes(value: Long): ByteString {
      val bytes = ByteArray(8)
      for (i in 0..7) {
        bytes[7 - i] = ((value shr (i * 8)) and 0xFF).toByte()
      }
      return ByteString.copyFrom(bytes)
    }

    private fun bytesToLong(bs: ByteString): Long {
      val b = bs.toByteArray()
      var result = 0L
      for (i in 0 until minOf(8, b.size)) {
        result = (result shl 8) or (b[i].toLong() and 0xFF)
      }
      return result
    }
  }
}
