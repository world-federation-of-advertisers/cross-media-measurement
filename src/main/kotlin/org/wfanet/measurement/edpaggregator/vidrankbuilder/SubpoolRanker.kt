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

package org.wfanet.measurement.edpaggregator.vidrankbuilder

import com.google.type.Date
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util.UUID
import java.util.logging.Logger
import kotlinx.coroutines.flow.collect
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.edpaggregator.rawimpressions.RankIndexStore
import org.wfanet.measurement.edpaggregator.rawimpressions.SubpoolFingerprintsStore
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankIndexBlobsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateRankIndexBlobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.createRankIndexBlobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRankIndexBlobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.rankIndexBlob
import org.wfanet.measurement.edpaggregator.vidlabeler.utils.Bytes12IntMap

/**
 * Ranks a **single** subpool: the per-subpool engine driven by [VidRankBuilder] for each subpool a
 * `RankerJob` covers.
 *
 * For one subpool it:
 * 1. loads the prior cumulative `SNAPSHOT` (if any) into a [RankAllocator] (rebuilding its rank
 *    `BitSet`), validating the blob checksum,
 * 2. materializes this dispatch's fingerprint set from the Phase-0 merged `SubpoolFingerprints`
 *    blob,
 * 3. runs [SubpoolRetention] to free aged-out ranks,
 * 4. allocates ranks to today's fingerprints (renewing already-ranked ones, capping at
 *    `ranked_size`),
 * 5. writes the new `DAY_ONLY` + `SNAPSHOT` blobs (DEK-encrypted) and records both as
 *    `RankIndexBlob` rows.
 *
 * @param subpoolFingerprintsStore reads the Phase-0 merged `SubpoolFingerprints` blob.
 * @param rankIndexStore reads the prior snapshot and writes the new rank-index blobs.
 * @param rankIndexBlobsStub metadata-storage service for locating the prior snapshot and inserting
 *   the new blob rows.
 * @param retention the data-deletion pass.
 * @param dataProvider EDP resource name (`dataProviders/{dp}`), parent of the upload wildcard for
 *   the prior-snapshot lookup.
 * @param rawImpressionUpload the upload resource name (parent of the new blob rows + key scoping).
 * @param modelLine the model line being ranked.
 * @param vidRankMapBlobPrefix static blob prefix for the vid-rank-map bucket.
 * @param kekUri KEK URI used to wrap each new blob's DEK.
 * @param encryptedSubpoolMapsDek DEK that decrypts the Phase-0 merged blobs.
 * @param maxEventDate newest event date this dispatch covers; stamped on the `DAY_ONLY` row.
 * @param metrics OpenTelemetry instruments.
 */
class SubpoolRanker(
  private val subpoolFingerprintsStore: SubpoolFingerprintsStore,
  private val rankIndexStore: RankIndexStore,
  private val rankIndexBlobsStub: RankIndexBlobServiceCoroutineStub,
  private val retention: SubpoolRetention,
  private val dataProvider: String,
  private val rawImpressionUpload: String,
  private val modelLine: String,
  private val vidRankMapBlobPrefix: String,
  private val kekUri: String,
  private val encryptedSubpoolMapsDek: EncryptedDek,
  private val maxEventDate: Date,
  private val metrics: VidRankBuilderMetrics = VidRankBuilderMetrics(),
) {
  /**
   * @property poolOffset the subpool ranked.
   * @property allocated ranks newly assigned to never-before-seen fingerprints.
   * @property renewed already-ranked fingerprints re-observed (rank preserved).
   * @property overflow fingerprints left unranked because the subpool was full.
   * @property freed ranks released by the retention pass.
   * @property cumulativeSize total ranked fingerprints after this dispatch.
   */
  data class Result(
    val poolOffset: Long,
    val allocated: Long,
    val renewed: Long,
    val overflow: Long,
    val freed: Long,
    val cumulativeSize: Long,
  )

  /** Ranks [poolOffset]; [subpoolBlobUri] is its Phase-0 merged blob; caps at [rankedSize]. */
  suspend fun rank(poolOffset: Long, subpoolBlobUri: String, rankedSize: Int): Result {
    val allocator = RankAllocator(poolOffset, rankedSize)

    // 1. Load the prior cumulative snapshot (validating its checksum), if one exists.
    val priorSnapshot = findPriorSnapshot(poolOffset)
    if (priorSnapshot != null) {
      allocator.loadFrom(
        rankIndexStore.readBlob(
          priorSnapshot.blobUri,
          priorSnapshot.encryptedDek,
          priorSnapshot.blobChecksum,
        )
      )
    }

    // 2. Materialize this dispatch's fingerprint set for the subpool from the Phase-0 merged blob.
    val todayFps = Bytes12IntMap()
    subpoolFingerprintsStore.readBlob(subpoolBlobUri, encryptedSubpoolMapsDek).collect { record ->
      val fps = record.fingerprints
      val count = fps.size() / FingerprintCodec.WIDTH
      var off = 0
      repeat(count) {
        todayFps.put(FingerprintCodec.readHi(fps, off), FingerprintCodec.readLo(fps, off + 8), 1)
        off += FingerprintCodec.WIDTH
      }
    }

    // 3. Retention: free aged-out ranks before allocating (freed slots become reusable today).
    retention.prune(poolOffset, allocator, todayFps)

    // 4. Allocate / renew ranks for today's fingerprints.
    todayFps.forEach { keyHi, keyLo, _ -> allocator.assign(keyHi, keyLo) }

    // 5. Write the new DAY_ONLY + SNAPSHOT blobs and record both as RankIndexBlob rows.
    val dek = rankIndexStore.generateDek(kekUri)
    val snapshotKey =
      RankIndexStore.snapshotKey(vidRankMapBlobPrefix, rawImpressionUpload, modelLine, poolOffset)
    val dayOnlyKey =
      RankIndexStore.dayOnlyKey(vidRankMapBlobPrefix, rawImpressionUpload, modelLine, poolOffset)
    val snapshotChecksum =
      rankIndexStore.writeBlob(snapshotKey, dek, allocator.streamCumulativeChunks())
    val dayOnlyChecksum = rankIndexStore.writeBlob(dayOnlyKey, dek, allocator.streamDayOnlyChunks())
    insertBlobRows(poolOffset, snapshotKey, snapshotChecksum, dayOnlyKey, dayOnlyChecksum, dek)

    recordMetrics(allocator)
    logger.info(
      "Subpool $poolOffset for $modelLine: allocated=${allocator.allocated}, " +
        "renewed=${allocator.renewed}, overflow=${allocator.overflow}, freed=${allocator.freed}, " +
        "cumulative=${allocator.cumulativeSize}"
    )
    return Result(
      poolOffset = poolOffset,
      allocated = allocator.allocated,
      renewed = allocator.renewed,
      overflow = allocator.overflow,
      freed = allocator.freed,
      cumulativeSize = allocator.cumulativeSize,
    )
  }

  /**
   * The newest non-deleted `SNAPSHOT` blob for [poolOffset] of this (data provider, model line)
   * across all uploads, or `null` on a cold (Day-1) subpool. The most recent `create_time` is the
   * current cumulative state (the design's N−1 recovery baseline).
   */
  private suspend fun findPriorSnapshot(poolOffset: Long): RankIndexBlob? {
    var latest: RankIndexBlob? = null
    rankIndexBlobsStub
      .listResources { pageToken: String ->
        val response =
          listRankIndexBlobs(
            listRankIndexBlobsRequest {
              parent = "$dataProvider/rawImpressionUploads/-"
              filter =
                ListRankIndexBlobsRequestKt.filter {
                  blobType = RankIndexBlob.BlobType.SNAPSHOT
                  cmmsModelLine = modelLine
                  this.poolOffset = poolOffset
                }
              this.pageToken = pageToken
            }
          )
        ResourceList(response.rankIndexBlobsList, response.nextPageToken)
      }
      .collect { page ->
        for (blob in page) {
          val current = latest
          if (
            current == null || blob.createTime.toComparable() > current.createTime.toComparable()
          ) {
            latest = blob
          }
        }
      }
    return latest
  }

  /** Inserts the SNAPSHOT + DAY_ONLY rows in one idempotent batch. */
  private suspend fun insertBlobRows(
    poolOffset: Long,
    snapshotKey: String,
    snapshotChecksum: com.google.protobuf.ByteString,
    dayOnlyKey: String,
    dayOnlyChecksum: com.google.protobuf.ByteString,
    dek: EncryptedDek,
  ) {
    rankIndexBlobsStub.batchCreateRankIndexBlobs(
      batchCreateRankIndexBlobsRequest {
        parent = rawImpressionUpload
        requests += createRankIndexBlobRequest {
          parent = rawImpressionUpload
          rankIndexBlob = rankIndexBlob {
            blobType = RankIndexBlob.BlobType.SNAPSHOT
            cmmsModelLine = modelLine
            this.poolOffset = poolOffset
            blobUri = snapshotKey
            blobChecksum = snapshotChecksum
            encryptedDek = dek
          }
          requestId = blobRequestId(poolOffset, RankIndexBlob.BlobType.SNAPSHOT)
        }
        requests += createRankIndexBlobRequest {
          parent = rawImpressionUpload
          rankIndexBlob = rankIndexBlob {
            blobType = RankIndexBlob.BlobType.DAY_ONLY
            cmmsModelLine = modelLine
            this.poolOffset = poolOffset
            blobUri = dayOnlyKey
            blobChecksum = dayOnlyChecksum
            encryptedDek = dek
            maxEventDate = this@SubpoolRanker.maxEventDate
          }
          requestId = blobRequestId(poolOffset, RankIndexBlob.BlobType.DAY_ONLY)
        }
      }
    )
  }

  private fun recordMetrics(allocator: RankAllocator) {
    metrics.ranksAllocatedCounter.add(allocator.allocated)
    metrics.ranksRenewedCounter.add(allocator.renewed)
    metrics.ranksFreedCounter.add(allocator.freed)
    metrics.overflowFingerprintsCounter.add(allocator.overflow)
    metrics.subpoolsRankedCounter.add(1)
  }

  /**
   * Deterministic UUID4 idempotency key for the (upload, model line, subpool, blob type) blob row,
   * stable across redeliveries so the batch insert reuses existing rows rather than duplicating.
   * Derived from an MD5 digest with the RFC-4122 version (4) and variant bits forced.
   */
  private fun blobRequestId(poolOffset: Long, blobType: RankIndexBlob.BlobType): String {
    val seed = "$rawImpressionUpload|$modelLine|$poolOffset|${blobType.name}"
    val bytes = MessageDigest.getInstance("MD5").digest(seed.toByteArray(Charsets.UTF_8))
    bytes[6] = ((bytes[6].toInt() and 0x0f) or 0x40).toByte() // version 4
    bytes[8] = ((bytes[8].toInt() and 0x3f) or 0x80).toByte() // variant 10xx
    val buffer = ByteBuffer.wrap(bytes)
    return UUID(buffer.long, buffer.long).toString()
  }

  companion object {
    private val logger = Logger.getLogger(SubpoolRanker::class.java.name)

    /** Orders timestamps for "newest snapshot" without depending on a proto comparator. */
    private fun com.google.protobuf.Timestamp.toComparable(): Long =
      seconds * 1_000_000_000L + nanos
  }
}
