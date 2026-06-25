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

package org.wfanet.measurement.edpaggregator.vidlabeler

import com.google.protobuf.util.Timestamps
import io.opentelemetry.api.common.Attributes
import java.util.logging.Logger
import kotlin.time.TimeSource
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.wfanet.measurement.common.api.ResourceKey
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.edpaggregator.rawimpressions.EventIdDigest
import org.wfanet.measurement.edpaggregator.rawimpressions.RankIndexStore
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankIndexBlobsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.listRankIndexBlobsRequest
import org.wfanet.measurement.edpaggregator.vidlabeler.utils.Bytes12IntMap
import org.wfanet.virtualpeople.common.RankAssignment
import org.wfanet.virtualpeople.common.rankAssignment

/**
 * In-memory per-subpool rank index for the memoized Phase-2 VID labeling path.
 *
 * Holds one [Bytes12IntMap] (`fingerprint -> rank`) per subpool, built once per WorkItem from the
 * current cumulative `SNAPSHOT` rank-index blobs of a `(dataProvider, modelLine)`. For each
 * impression the labeler looks up the impression's 12-byte [EventIdDigest] (the same key the
 * Phase-0 `SubpoolAssigner` and Phase-1 `VidRankBuilder` use) and, when the fingerprint holds a
 * rank, emits a [RankAssignment] for the owning subpool. The caller attaches it to the
 * `LabelerInput` so the model's `RankedPopulationNode` leaf derives a collision-free VID via
 * Feistel; a miss leaves the input untouched and the leaf falls back to the hash path.
 *
 * Built once and read-only thereafter: [lookup] only reads the underlying [Bytes12IntMap]s, so it
 * is safe to call concurrently from the reader's CPU pool even though [Bytes12IntMap] is not safe
 * for concurrent *mutation*.
 *
 * @property mapsByPoolOffset the per-subpool `fingerprint -> rank` maps, keyed by `pool_offset`.
 */
class MemoizedRankIndex
private constructor(private val mapsByPoolOffset: Map<Long, Bytes12IntMap>) {

  /** Number of subpools loaded. Exposed for diagnostics / tests. */
  val subpoolCount: Int
    get() = mapsByPoolOffset.size

  /**
   * Returns a [RankAssignment] for every subpool whose map holds [digest] — one `(pool_offset,
   * local_rank)` per subpool the fingerprint is ranked in — or an empty list when it holds no rank
   * in any subpool (an overflow / unseen fingerprint, which the labeler hashes instead).
   *
   * All matches are returned (not just the first) because a fingerprint can legitimately route to
   * different subpools across impressions and therefore appear in several subpool snapshots. The
   * caller attaches all of them; the model's `RankedPopulationNode` leaf selects the one matching
   * its own `pool_offset` and ignores the rest. Returning only the first match would attach a rank
   * for the wrong subpool when the impression reaches a different one, which the leaf rejects.
   */
  fun lookup(digest: EventIdDigest): List<RankAssignment> = buildList {
    for ((poolOffset, map) in mapsByPoolOffset) {
      val rank = map.get(digest.high, digest.low)
      if (rank != Bytes12IntMap.NOT_PRESENT) {
        add(
          rankAssignment {
            this.poolOffset = poolOffset
            localRank = rank.toLong()
          }
        )
      }
    }
  }

  companion object {
    private val logger = Logger.getLogger(MemoizedRankIndex::class.java.name)

    /** Default number of subpool blobs decrypted/built concurrently. */
    const val DEFAULT_READER_PARALLELISM: Int = 16

    private const val FINGERPRINT_BYTES = Bytes12IntMap.FINGERPRINT_BYTE_WIDTH

    /**
     * Loads the current rank index for [modelLine] under [dataProvider].
     *
     * Discovery mirrors the Phase-1 ranker's prior-snapshot resolution: list every non-deleted
     * `SNAPSHOT` blob for the `(dataProvider, modelLine)` across all uploads and keep, per
     * `pool_offset`, the one with the greatest `create_time` (the current cumulative state — a
     * backfill writes a `SNAPSHOT` too, so newest `create_time` is always the most complete). Each
     * selected blob is then decrypted and decoded into a [Bytes12IntMap], up to [readerParallelism]
     * blobs at a time.
     *
     * @param rankIndexBlobsStub stub used to locate the per-subpool `SNAPSHOT` blob pointers.
     * @param rankIndexStore reads + decrypts the rank-index blob bytes (keyed to the vid-rank-map
     *   storage).
     * @param dataProvider the EDP resource name (`dataProviders/{dp}`).
     * @param modelLine the model line being labeled.
     */
    suspend fun load(
      rankIndexBlobsStub: RankIndexBlobServiceCoroutineStub,
      rankIndexStore: RankIndexStore,
      dataProvider: String,
      modelLine: String,
      readerParallelism: Int = DEFAULT_READER_PARALLELISM,
      metrics: MemoizedRankIndexMetrics = MemoizedRankIndexMetrics(),
    ): MemoizedRankIndex {
      val startMark = TimeSource.Monotonic.markNow()
      val latestByPoolOffset = LinkedHashMap<Long, RankIndexBlob>()
      rankIndexBlobsStub
        .listResources { pageToken: String ->
          val response =
            listRankIndexBlobs(
              listRankIndexBlobsRequest {
                parent = "$dataProvider/rawImpressionUploads/${ResourceKey.WILDCARD_ID}"
                filter =
                  ListRankIndexBlobsRequestKt.filter {
                    blobType = RankIndexBlob.BlobType.SNAPSHOT
                    cmmsModelLine = modelLine
                  }
                this.pageToken = pageToken
              }
            )
          ResourceList(response.rankIndexBlobsList, response.nextPageToken)
        }
        .collect { page ->
          for (blob in page) {
            val current = latestByPoolOffset[blob.poolOffset]
            if (current == null || Timestamps.compare(blob.createTime, current.createTime) > 0) {
              latestByPoolOffset[blob.poolOffset] = blob
            }
          }
        }

      val semaphore = Semaphore(readerParallelism.coerceAtLeast(1))
      val loaded: Map<Long, LoadedSubpool> = coroutineScope {
        latestByPoolOffset.values
          .map { blob ->
            async {
              semaphore.withPermit { blob.poolOffset to readSubpoolMap(rankIndexStore, blob) }
            }
          }
          .awaitAll()
          .toMap()
      }
      val maps: Map<Long, Bytes12IntMap> = loaded.mapValues { (_, subpool) -> subpool.map }

      val baseAttributes =
        Attributes.of(metrics.DATA_PROVIDER_ATTR, dataProvider, metrics.MODEL_LINE_ATTR, modelLine)
      metrics.subpoolCountGauge.set(maps.size.toLong(), baseAttributes)
      metrics.coldStartDurationHistogram.record(
        startMark.elapsedNow().inWholeMilliseconds / 1000.0,
        baseAttributes,
      )
      for ((poolOffset, subpool) in loaded) {
        val poolAttributes =
          baseAttributes.toBuilder().put(metrics.POOL_OFFSET_ATTR, poolOffset).build()
        metrics.entryCountGauge.set(subpool.map.size, poolAttributes)
        metrics.rankedSizeGauge.set(subpool.rankedSize.toLong(), poolAttributes)
      }

      logger.info("Loaded memoized rank index for $modelLine: ${maps.size} subpool(s)")
      return MemoizedRankIndex(maps)
    }

    /**
     * Decrypts [blob] and decodes its packed `(fingerprint, rank)` records into a [Bytes12IntMap].
     */
    private suspend fun readSubpoolMap(
      rankIndexStore: RankIndexStore,
      blob: RankIndexBlob,
    ): LoadedSubpool {
      val map = Bytes12IntMap()
      var rankedSize = 0
      rankIndexStore.readBlob(blob.blobUri, blob.encryptedDek, blob.blobChecksum).collect { record
        ->
        rankedSize = record.rankedSize
        val fingerprints = record.fingerprints
        val ranks = record.ranksList
        var offset = 0
        for (i in ranks.indices) {
          map.put(
            fingerprints.substring(offset, offset + FINGERPRINT_BYTES).toByteArray(),
            ranks[i],
          )
          offset += FINGERPRINT_BYTES
        }
      }
      return LoadedSubpool(map, rankedSize)
    }

    /** A subpool's in-memory rank map plus its configured `ranked_size`, captured at load. */
    private data class LoadedSubpool(val map: Bytes12IntMap, val rankedSize: Int)
  }
}
