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
import org.wfanet.measurement.edpaggregator.vidrankbuilder.EventIdDigestBytes
import org.wfanet.virtualpeople.common.LabelerInput
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
 * The per-subpool state is held as two parallel arrays ([poolOffsets] + [maps]) rather than a
 * `Map<Long, Bytes12IntMap>` so the per-impression probe iterates primitive indices with no map
 * entry / iterator allocation on the hot path. Index `i` pairs `poolOffsets[i]` with `maps[i]`; the
 * arrays share the load-time subpool order.
 *
 * @property poolOffsets the loaded subpools' `pool_offset`s, parallel to [maps].
 * @property maps the per-subpool `fingerprint -> rank` maps, parallel to [poolOffsets].
 */
class MemoizedRankIndex
private constructor(
  private val poolOffsets: LongArray,
  private val maps: Array<Bytes12IntMap>,
  /**
   * KEK URI shared by every loaded `RankIndexBlob` (the EDP's own KEK). Reused to wrap the
   * labeled-output DEK, so the output is encrypted with the same key as the rank-index blobs.
   */
  val kekUri: String,
) {

  /** Number of subpools loaded. Exposed for diagnostics / tests. */
  val subpoolCount: Int
    get() = poolOffsets.size

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
   *
   * Kept for tests and callers that want the list; the hot labeling path uses
   * [appendRankAssignments] to avoid the intermediate list + per-match message allocations.
   */
  fun lookup(digest: EventIdDigest): List<RankAssignment> = buildList {
    for (i in poolOffsets.indices) {
      val rank = maps[i].get(digest.high, digest.low)
      if (rank != Bytes12IntMap.NOT_PRESENT) {
        add(
          rankAssignment {
            poolOffset = poolOffsets[i]
            localRank = rank.toLong()
          }
        )
      }
    }
  }

  /**
   * Appends a `RankAssignment` for every subpool whose map holds [digest] directly onto [builder]'s
   * `rank_assignments`, and returns how many were appended. Equivalent to attaching [lookup]'s
   * result but without allocating the intermediate list or the per-match [RankAssignment] messages
   * (they are built in place). Probes every subpool in the same order as [lookup]; a return of `0`
   * means the fingerprint is unranked (overflow / unseen).
   */
  fun appendRankAssignments(digest: EventIdDigest, builder: LabelerInput.Builder): Int {
    var appended = 0
    for (i in poolOffsets.indices) {
      val rank = maps[i].get(digest.high, digest.low)
      if (rank != Bytes12IntMap.NOT_PRESENT) {
        builder.addRankAssignmentsBuilder().apply {
          poolOffset = poolOffsets[i]
          localRank = rank.toLong()
        }
        appended++
      }
    }
    return appended
  }

  /**
   * Content identity of a loaded index: the `(dataProvider, modelLine)` plus the sorted set of
   * chosen `SNAPSHOT` blob URIs. `RankIndexBlob` URIs are per-write-unique (attempt UUID) and their
   * content is immutable, so an identical set of chosen URIs would build a byte-identical index —
   * which is what lets [MemoizedRankIndexCache] safely reuse one across WorkItems. A new Phase-1
   * snapshot for any subpool produces a new chosen URI, hence a new [Key] and a rebuild.
   */
  data class Key(val dataProvider: String, val modelLine: String, val blobUris: List<String>)

  /** The cheap listing result of [resolveLatestBlobs]: the cache [key] and the chosen blobs. */
  class ResolvedBlobs(val key: Key, val blobs: List<RankIndexBlob>)

  companion object {
    private val logger = Logger.getLogger(MemoizedRankIndex::class.java.name)

    /** Default number of subpool blobs decrypted/built concurrently. */
    const val DEFAULT_READER_PARALLELISM: Int = 16

    private const val FINGERPRINT_BYTES = Bytes12IntMap.FINGERPRINT_BYTE_WIDTH

    /**
     * Builds an index directly from prebuilt per-subpool `fingerprint -> rank` maps. Visible for
     * testing consumers of [lookup] (e.g. `VidLabelingSink`) without standing up the
     * `RankIndexBlobService` / `RankIndexStore`.
     */
    fun fromMaps(
      mapsByPoolOffset: Map<Long, Bytes12IntMap>,
      kekUri: String = "test-kek-uri",
    ): MemoizedRankIndex =
      MemoizedRankIndex(
        mapsByPoolOffset.keys.toLongArray(),
        mapsByPoolOffset.values.toTypedArray(),
        kekUri,
      )

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
     * Memory: the entire cumulative index is held in heap for the WorkItem's lifetime — every
     * subpool's latest `SNAPSHOT` is decoded into a [Bytes12IntMap] and retained until the WorkItem
     * finishes. There is no runtime size bound; capacity is a deployment assumption (~6 GiB
     * steady-state on the 128 GB VID-labeler VM). Size the VM for the model line's total
     * fingerprint count; reducing VM memory can OOM here, and bounding the resident set would
     * require a redesign (e.g. a sharded/streaming lookup).
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
      val resolved = resolveLatestBlobs(rankIndexBlobsStub, dataProvider, modelLine)
      return buildFrom(
        rankIndexStore,
        dataProvider,
        modelLine,
        resolved.blobs,
        readerParallelism,
        metrics,
      )
    }

    /**
     * The cheap discovery half of [load]: lists every non-deleted `SNAPSHOT` blob for the
     * `(dataProvider, modelLine)` across all uploads, keeps per `pool_offset` the one with the
     * greatest `create_time` (tie-broken by name), and returns those chosen blobs together with a
     * content-identity [Key]. No blob bytes are downloaded here — this is the part that runs on
     * EVERY WorkItem so [MemoizedRankIndexCache] can detect a changed snapshot set. Throws if no
     * SNAPSHOT exists (the same fail-loud contract [load] had).
     */
    suspend fun resolveLatestBlobs(
      rankIndexBlobsStub: RankIndexBlobServiceCoroutineStub,
      dataProvider: String,
      modelLine: String,
    ): ResolvedBlobs {
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
            if (current == null || isNewerSnapshot(blob, current)) {
              latestByPoolOffset[blob.poolOffset] = blob
            }
          }
        }

      // Phase-1 always precedes Phase-2 and must persist at least one cumulative SNAPSHOT before a
      // memoized model line is labeled. Zero snapshots here is a real anomaly (misconfig or lost
      // Phase-1 output): fail loud rather than silently degrading every impression to the hash path
      // (which also strands #4083's output-KEK resolution, since it reuses the KEK on these blobs).
      check(latestByPoolOffset.isNotEmpty()) {
        "No SNAPSHOT rank-index blobs for memoized model line $modelLine under $dataProvider; " +
          "Phase-1 must produce a cumulative snapshot before Phase-2 labeling"
      }

      val blobs = latestByPoolOffset.values.toList()
      val key = Key(dataProvider, modelLine, blobs.map { it.blobUri }.sorted())
      return ResolvedBlobs(key, blobs)
    }

    /**
     * The expensive build half of [load]: decrypts + decodes each chosen [blobs] entry into a
     * [Bytes12IntMap] (up to [readerParallelism] at a time) and assembles the [MemoizedRankIndex].
     * Records the cold-start / per-subpool metrics. Split out so [MemoizedRankIndexCache] can skip
     * it on a key match while still running the cheap [resolveLatestBlobs] every WorkItem.
     *
     * Memory: the entire cumulative index is held in heap for the WorkItem's lifetime (~6 GiB
     * steady-state on the 128 GB VID-labeler VM). Size the VM for the model line's fingerprint
     * count.
     */
    suspend fun buildFrom(
      rankIndexStore: RankIndexStore,
      dataProvider: String,
      modelLine: String,
      blobs: List<RankIndexBlob>,
      readerParallelism: Int = DEFAULT_READER_PARALLELISM,
      metrics: MemoizedRankIndexMetrics = MemoizedRankIndexMetrics(),
    ): MemoizedRankIndex {
      val startMark = TimeSource.Monotonic.markNow()
      val semaphore = Semaphore(readerParallelism.coerceAtLeast(1))
      val loaded: Map<Long, LoadedSubpool> = coroutineScope {
        blobs
          .map { blob ->
            async {
              semaphore.withPermit { blob.poolOffset to readSubpoolMap(rankIndexStore, blob) }
            }
          }
          .awaitAll()
          .toMap()
      }
      // Parallel arrays in the load-time subpool order (loaded is an ordered LinkedHashMap), so
      // poolOffsets[i] pairs with subpoolMaps[i]. Held as arrays (not a Map) for an allocation-free
      // per-impression probe.
      val poolOffsets: LongArray = loaded.keys.toLongArray()
      val subpoolMaps: Array<Bytes12IntMap> = loaded.values.map { it.map }.toTypedArray()

      val baseAttributes =
        Attributes.of(metrics.DATA_PROVIDER_ATTR, dataProvider, metrics.MODEL_LINE_ATTR, modelLine)
      metrics.subpoolCountGauge.set(poolOffsets.size.toLong(), baseAttributes)
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

      // Every rank-index blob is wrapped with the EDP's single KEK; resolve it (and assert it is
      // present + consistent) so the labeled output can be wrapped with the same key. The caller
      // (resolveLatestBlobs) already guarantees there is at least one blob to resolve it from.
      val kekUri = resolveKekUri(blobs, modelLine)

      logger.info("Loaded memoized rank index for $modelLine: ${poolOffsets.size} subpool(s)")
      return MemoizedRankIndex(poolOffsets, subpoolMaps, kekUri)
    }

    /**
     * Returns the single KEK URI shared by [blobs] (the EDP's own KEK). Throws if any blob has an
     * empty `encrypted_dek.kek_uri` or if the blobs disagree, since the labeled output must be
     * wrapped with exactly one KEK.
     */
    private fun resolveKekUri(blobs: Collection<RankIndexBlob>, modelLine: String): String {
      val kekUris = blobs.map { it.encryptedDek.kekUri }.toSet()
      check(kekUris.size == 1 && kekUris.first().isNotEmpty()) {
        "rank-index blobs for $modelLine have missing or inconsistent KEK URIs: $kekUris"
      }
      return kekUris.first()
    }

    /**
     * Decrypts [blob] and decodes its packed `(fingerprint, rank)` records into a [Bytes12IntMap].
     */
    private suspend fun readSubpoolMap(
      rankIndexStore: RankIndexStore,
      blob: RankIndexBlob,
    ): LoadedSubpool {
      // Open the blob ONCE: openBlob does the single getBlob and hands back both the byte size (for
      // pre-sizing) and the record flow (for content), instead of a getBlob for blobSize plus a
      // second getBlob inside readBlob. An absent blob (null) is handled exactly as before: no
      // records are read, so the ranked_size check below fails with the same error.
      val opened = rankIndexStore.openBlob(blob.blobUri, blob.encryptedDek, blob.blobChecksum)
      val estimatedEntries = (opened?.first ?: 0L) / RankIndexStore.ON_DISK_BYTES_PER_ENTRY
      // Size the map to hold estimatedEntries WITHOUT resizing: capacity = entries / load-factor
      // (0.75) == entries * 4 / 3, floored at the minimum. The map is lookup-only (never iterated),
      // so a different capacity cannot change any observable result — only avoids resizes at load.
      val map = Bytes12IntMap((estimatedEntries * 4 / 3).coerceAtLeast(Bytes12IntMap.MIN_CAPACITY))
      var rankedSize = -1
      opened?.second?.collect { record ->
        if (rankedSize == -1) {
          rankedSize = record.rankedSize
        } else {
          check(rankedSize == record.rankedSize) {
            "Inconsistent ranked_size in rank index blob ${blob.blobUri}: " +
              "$rankedSize vs ${record.rankedSize}"
          }
        }
        val fingerprints = record.fingerprints
        // `rank_index_map.proto`: `fingerprints` length MUST equal `ranks_count` * 12. Enforce it
        // so
        // a malformed blob fails with a clear, attributable error instead of an opaque
        // IndexOutOfBoundsException (too short) or silently dropped trailing fingerprints (too
        // long).
        check(fingerprints.size() == record.ranksCount * FINGERPRINT_BYTES) {
          "Malformed rank index blob ${blob.blobUri}: ${fingerprints.size()} fingerprint bytes " +
            "for ${record.ranksCount} ranks (expected ${record.ranksCount * FINGERPRINT_BYTES})"
        }
        // Read fingerprint hi/lo + the primitive rank directly. The old per-entry
        // `substring(...).toByteArray()` + boxed `ranksList` allocated ~2 objects per entry, which
        // GC-thrashed the index load into a crawl at 4B scale (hundreds of millions of entries).
        var offset = 0
        for (i in 0 until record.ranksCount) {
          map.put(
            EventIdDigestBytes.readHi(fingerprints, offset),
            EventIdDigestBytes.readLo(fingerprints, offset + 8),
            record.getRanks(i),
          )
          offset += FINGERPRINT_BYTES
        }
      }
      check(rankedSize >= 0) { "Empty rank index blob ${blob.blobUri} has no ranked_size" }
      return LoadedSubpool(map, rankedSize)
    }

    /**
     * Whether [candidate] should win over the [current] chosen SNAPSHOT for a subpool: the greatest
     * `create_time`, breaking ties by the (unique) resource [RankIndexBlob.name] so two SNAPSHOTs
     * written at the same instant pick a deterministic winner rather than a listing-order-dependent
     * one.
     */
    private fun isNewerSnapshot(candidate: RankIndexBlob, current: RankIndexBlob): Boolean {
      val comparison = Timestamps.compare(candidate.createTime, current.createTime)
      return comparison > 0 || (comparison == 0 && candidate.name > current.name)
    }

    /** A subpool's in-memory rank map plus its configured `ranked_size`, captured at load. */
    private data class LoadedSubpool(val map: Bytes12IntMap, val rankedSize: Int)
  }
}
