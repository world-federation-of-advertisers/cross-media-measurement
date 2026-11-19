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

import io.grpc.StatusException
import io.opentelemetry.api.common.Attributes
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext
import kotlin.time.TimeSource
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.wfanet.measurement.common.api.ResourceKey
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.edpaggregator.rawimpressions.EventIdDigest
import org.wfanet.measurement.edpaggregator.rawimpressions.RankIndexStore
import org.wfanet.measurement.edpaggregator.service.RankIndexBlobKey
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadKey
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
 * saved `DAY_ONLY` rank-index blobs of one upload and model line. For each impression the labeler
 * looks up the impression's 12-byte [EventIdDigest] (the same key the Phase-0 `SubpoolAssigner` and
 * Phase-1 `VidRankBuilder` use) and, when the fingerprint holds a rank, emits a [RankAssignment]
 * for the owning subpool. The caller attaches it to the `LabelerInput` so the model's
 * `RankedPopulationNode` leaf derives a collision-free VID via Feistel; a miss leaves the input
 * untouched and the leaf falls back to the hash path.
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

  /** Identity of one immutable, upload-owned subpool blob. */
  data class BlobKey(val poolOffset: Long, val blobUri: String)

  /** Content identity of an upload's selected rank indexes, sorted by pool offset. */
  data class Key(
    val dataProvider: String,
    val modelLine: String,
    val rawImpressionUpload: String,
    val blobs: List<BlobKey>,
  )

  /** The cheap discovery result, including the exact content key. */
  class ResolvedBlobs(val key: Key, val blobs: List<RankIndexBlob>)

  companion object {
    private val logger = Logger.getLogger(MemoizedRankIndex::class.java.name)

    /** Default number of subpool blobs decrypted/built concurrently. */
    // TODO(world-federation-of-advertisers/cross-media-measurement#4741): Make reader parallelism a
    //  VID-labeler deployment setting instead of always using this default.
    const val DEFAULT_READER_PARALLELISM: Int = 16

    private const val FINGERPRINT_BYTES = Bytes12IntMap.FINGERPRINT_BYTE_WIDTH

    /**
     * Records between per-subpool progress logs. VidRankBuilder writes ~1M entries per record, so
     * this logs roughly every 8M entries — a few lines per subpool, enough to locate a stall.
     */
    private const val PROGRESS_LOG_RECORD_INTERVAL = 8L

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

    /** Loads the saved labeling assignments for one upload and model line. */
    suspend fun load(
      rankIndexBlobsStub: RankIndexBlobServiceCoroutineStub,
      rankIndexStore: RankIndexStore,
      dataProvider: String,
      modelLine: String,
      metadataReadThrottler: Throttler,
      rawImpressionUpload: String,
      expectedPoolOffsets: Set<Long>,
      readerParallelism: Int = DEFAULT_READER_PARALLELISM,
      metrics: MemoizedRankIndexMetrics = MemoizedRankIndexMetrics(),
      loadContext: CoroutineContext = Dispatchers.Default,
    ): MemoizedRankIndex {
      val resolved =
        resolveUploadBlobs(
          rankIndexBlobsStub,
          dataProvider,
          modelLine,
          metadataReadThrottler,
          rawImpressionUpload,
          expectedPoolOffsets,
        )
      return buildFrom(
        rankIndexStore,
        dataProvider,
        modelLine,
        resolved.blobs,
        readerParallelism,
        metrics,
        loadContext,
      )
    }

    /** Resolves exactly one DAY_ONLY blob for every subpool discovered for this upload. */
    suspend fun resolveUploadBlobs(
      rankIndexBlobsStub: RankIndexBlobServiceCoroutineStub,
      dataProvider: String,
      modelLine: String,
      metadataReadThrottler: Throttler,
      rawImpressionUpload: String,
      expectedPoolOffsets: Set<Long>,
    ): ResolvedBlobs {
      val uploadKey =
        requireNotNull(RawImpressionUploadKey.fromName(rawImpressionUpload)) {
          "Malformed raw impression upload: $rawImpressionUpload"
        }
      require(uploadKey.rawImpressionUploadId != ResourceKey.WILDCARD_ID) {
        "Rank indexes require an exact upload"
      }
      require(dataProvider == "dataProviders/${uploadKey.dataProviderId}") {
        "Rank-index upload does not belong to $dataProvider"
      }
      check(expectedPoolOffsets.isNotEmpty()) {
        "No expected ranked subpools for $rawImpressionUpload under $modelLine"
      }
      val byPoolOffset = LinkedHashMap<Long, RankIndexBlob>()
      rankIndexBlobsStub
        .listResources { pageToken: String ->
          val response =
            try {
              metadataReadThrottler.onReady {
                rankIndexBlobsStub.listRankIndexBlobs(
                  listRankIndexBlobsRequest {
                    parent = rawImpressionUpload
                    filter =
                      ListRankIndexBlobsRequestKt.filter {
                        blobType = RankIndexBlob.BlobType.DAY_ONLY
                        cmmsModelLine = modelLine
                      }
                    this.pageToken = pageToken
                  }
                )
              }
            } catch (e: StatusException) {
              throw IllegalStateException(
                "Unable to resolve DAY_ONLY rank indexes for $rawImpressionUpload",
                e,
              )
            }
          ResourceList(response.rankIndexBlobsList, response.nextPageToken)
        }
        .collect { page ->
          for (blob in page) {
            val blobKey =
              checkNotNull(RankIndexBlobKey.fromName(blob.name)) {
                "Malformed rank-index resource: ${blob.name}"
              }
            check(
              blobKey.parentKey.toName() == rawImpressionUpload &&
                blob.cmmsModelLine == modelLine &&
                blob.blobType == RankIndexBlob.BlobType.DAY_ONLY &&
                !blob.hasDeleteTime()
            ) {
              "Rank-index response contains an unexpected owner, model line or blob type"
            }
            check(blob.hasPoolOffset() && blob.poolOffset in expectedPoolOffsets) {
              "Unexpected rank-index pool offset ${blob.poolOffset}"
            }
            check(byPoolOffset.put(blob.poolOffset, blob) == null) {
              "Duplicate DAY_ONLY rank-index pool offset ${blob.poolOffset}"
            }
          }
        }
      val missingOffsets = expectedPoolOffsets - byPoolOffset.keys
      check(missingOffsets.isEmpty()) {
        "Missing DAY_ONLY rank-index blobs for pool offsets $missingOffsets in $rawImpressionUpload"
      }
      val blobs = byPoolOffset.values.sortedBy { it.poolOffset }
      val key =
        Key(
          dataProvider,
          modelLine,
          rawImpressionUpload,
          blobs.map { BlobKey(it.poolOffset, it.blobUri) },
        )
      return ResolvedBlobs(key, blobs)
    }

    /**
     * The expensive build half of [load]: decrypts + decodes each chosen [blobs] entry into a
     * [Bytes12IntMap] (up to [readerParallelism] at a time) and assembles the [MemoizedRankIndex].
     * Records the cold-start / per-subpool metrics. Split out so [MemoizedRankIndexCache] can skip
     * it on a key match while still running the cheap [resolveUploadBlobs] every WorkItem.
     *
     * Memory: this upload's rank indexes are held in heap for the WorkItem's lifetime. Size the VM
     * for the upload's ranked fingerprint count.
     *
     * Subpools load in [loadContext] (default [Dispatchers.Default]) so up to [readerParallelism]
     * decrypt/decode/insert pipelines actually run on separate cores, rather than sharing the
     * caller's thread — which, under a dispatcher-less `runBlocking`, serializes the whole build.
     * Each subpool owns its own [Bytes12IntMap], so concurrent loads never share mutable state.
     * [loadContext] must not contain a [Job]: that would replace the build scope as the loads'
     * parent, letting them outlive a cancelled attempt or a sibling load's failure.
     */
    suspend fun buildFrom(
      rankIndexStore: RankIndexStore,
      dataProvider: String,
      modelLine: String,
      blobs: List<RankIndexBlob>,
      readerParallelism: Int = DEFAULT_READER_PARALLELISM,
      metrics: MemoizedRankIndexMetrics = MemoizedRankIndexMetrics(),
      loadContext: CoroutineContext = Dispatchers.Default,
    ): MemoizedRankIndex {
      require(loadContext[Job] == null) { "loadContext must not contain a Job" }
      val startMark = TimeSource.Monotonic.markNow()
      logger.info(
        "Building memoized rank index for $modelLine: ${blobs.size} subpool blob(s), " +
          "readerParallelism=$readerParallelism"
      )
      val semaphore = Semaphore(readerParallelism.coerceAtLeast(1))
      val loaded: Map<Long, LoadedSubpool> = coroutineScope {
        blobs
          .map { blob ->
            async(loadContext) {
              semaphore.withPermit { blob.poolOffset to readSubpoolMap(rankIndexStore, blob) }
            }
          }
          .awaitAll()
          .toMap()
      }
      // Parallel arrays in the load-time subpool order (loaded is an ordered LinkedHashMap from
      // awaitAll().toMap()), so poolOffsets[i] pairs with subpoolMaps[i]. Both lookup() and
      // appendRankAssignments() walk poolOffsets.indices in exactly this order; that stability
      // relies on loaded staying insertion-ordered and would silently change if the map type
      // changed. The model leaf selects by pool_offset, so any order is still correct — this only
      // affects reproducible load-time ordering. Held as arrays (not a Map) for an allocation-free
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
        subpool.rankedSize?.let { rankedSize ->
          metrics.rankedSizeGauge.set(rankedSize.toLong(), poolAttributes)
        }
      }

      // Every selected rank-index blob is wrapped with the EDP's single KEK; resolve it (and assert
      // it is
      // present + consistent) so the labeled output can be wrapped with the same key. The caller
      // (resolveUploadBlobs) already guarantees there is at least one blob to resolve it from.
      val kekUri = resolveKekUri(blobs, modelLine)

      logger.info(
        "Loaded memoized rank index for $modelLine: ${poolOffsets.size} subpool(s) in " +
          "${startMark.elapsedNow().inWholeMilliseconds} ms"
      )
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
      // second getBlob inside readBlob. Missing objects fail; a valid empty DAY_ONLY object
      // supplies no record header and leaves the optional ranked size unset.
      val startMark = TimeSource.Monotonic.markNow()
      logger.info("Loading rank index subpool ${blob.poolOffset} from ${blob.blobUri}")
      check(blob.blobType == RankIndexBlob.BlobType.DAY_ONLY) { "Expected DAY_ONLY rank indexes" }
      check(!blob.blobChecksum.isEmpty && blob.encryptedDek.kekUri.isNotEmpty()) {
        "Rank index ${blob.name} is missing its checksum or encryption key"
      }
      val opened =
        checkNotNull(rankIndexStore.openBlob(blob.blobUri, blob.encryptedDek, blob.blobChecksum)) {
          "Rank-index blob ${blob.blobUri} does not exist"
        }
      val estimatedEntries = opened.first / RankIndexStore.ON_DISK_BYTES_PER_ENTRY
      // Size the map to hold estimatedEntries WITHOUT resizing: capacity = entries / load-factor
      // (0.75) == entries * 4 / 3, floored at the minimum. The map is lookup-only (never iterated),
      // so a different capacity cannot change any observable result — only avoids resizes at load.
      val map = Bytes12IntMap((estimatedEntries * 4 / 3).coerceAtLeast(Bytes12IntMap.MIN_CAPACITY))
      // Setup (key unwrap, blob open, map allocation) is logged separately from reading and
      // inserting records, so a stall in either is attributable.
      val setupMs = startMark.elapsedNow().inWholeMilliseconds
      logger.info(
        "Rank index subpool ${blob.poolOffset} setup complete after $setupMs ms: " +
          "${opened.first} bytes on disk, ~$estimatedEntries estimated entries, " +
          "map capacity ${map.capacity()}"
      )
      var rankedSize: Int? = null
      var records = 0L
      var entries = 0L
      var plaintextBytes = 0L
      opened.second.collect { record ->
        check(record.poolOffset == blob.poolOffset && record.rankedSize >= 0) {
          "Invalid pool offset or ranked_size in ${blob.blobUri}"
        }
        if (rankedSize == null) {
          rankedSize = record.rankedSize
        } else {
          check(rankedSize == record.rankedSize) {
            "Inconsistent ranked_size in rank index blob ${blob.blobUri}: " +
              "$rankedSize vs ${record.rankedSize}"
          }
        }
        val fingerprints = record.fingerprints
        // `rank_index_map.proto`: `fingerprints` length MUST equal `ranks_count` * 12. Enforce it
        // so a malformed blob fails with a clear, attributable error instead of an opaque
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
          val high = EventIdDigestBytes.readHi(fingerprints, offset)
          val low = EventIdDigestBytes.readLo(fingerprints, offset + 8)
          val rank = record.getRanks(i)
          check(
            rank in 0 until record.rankedSize && map.get(high, low) == Bytes12IntMap.NOT_PRESENT
          ) {
            "Invalid or duplicate rank-index entry in ${blob.blobUri}"
          }
          map.put(high, low, rank)
          offset += FINGERPRINT_BYTES
        }
        records++
        entries += record.ranksCount
        plaintextBytes += record.serializedSize
        if (records % PROGRESS_LOG_RECORD_INTERVAL == 0L) {
          val elapsedMs = startMark.elapsedNow().inWholeMilliseconds
          logger.info(
            "Rank index subpool ${blob.poolOffset}: $records record(s), $entries entries, " +
              "$plaintextBytes plaintext bytes after $elapsedMs ms"
          )
        }
      }
      logger.info(
        "Loaded rank index subpool ${blob.poolOffset}: $records record(s), $entries entries, " +
          "$plaintextBytes plaintext bytes in ${startMark.elapsedNow().inWholeMilliseconds} ms"
      )
      return LoadedSubpool(map, rankedSize)
    }

    /** A subpool's in-memory rank map plus its configured `ranked_size`, captured at load. */
    private data class LoadedSubpool(val map: Bytes12IntMap, val rankedSize: Int?)
  }
}
