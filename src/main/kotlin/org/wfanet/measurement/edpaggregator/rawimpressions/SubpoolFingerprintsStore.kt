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

package org.wfanet.measurement.edpaggregator.rawimpressions

import com.google.crypto.tink.KmsClient
import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.SubpoolFingerprints
import org.wfanet.measurement.edpaggregator.v1alpha.subpoolFingerprints
import org.wfanet.measurement.storage.StorageClient

/**
 * Reads and writes the per-subpool `SubpoolFingerprints` maps as **RecordIO** blobs.
 *
 * Lives in `rawimpressions` because both Phase-0 (`SubpoolAssigner`) and Phase-1 (`VidRankBuilder`)
 * use it: Phase-0 writes the per-(shard, subpool) blobs and (last-shard-out) merges them into one
 * per-subpool blob; Phase-1 reads the merged blobs. The DEK-generation and encrypted-client
 * plumbing is shared with [RankIndexStore] via [EncryptedRecordIoStore].
 *
 * ## Why RecordIO (and not a single `SubpoolFingerprints` message)
 *
 * A protobuf message — and a Java `ByteString` — is capped at ~2 GB (`Int`-indexed), i.e. ~178M
 * 12-byte fingerprints. A merged per-subpool blob is the **un-sharded** subpool and can exceed
 * that. So a blob is a **stream of `SubpoolFingerprints` records**: the caller supplies one
 * packed-fingerprints chunk per record ([writeBlob] takes a [Flow]), and reads stream back
 * record-by-record — bounded memory regardless of subpool size.
 *
 * ## Encryption
 *
 * Every blob is envelope-encrypted with a per-writer DEK (see [EncryptedRecordIoStore]). DEK
 * *persistence* is not the store's concern — the SubpoolAssigner stores it on the
 * `PoolAssignmentJob` row and the last-shard-out recovers every shard's DEK with
 * `ListPoolAssignmentJobs`.
 *
 * @param storageClient the base (unencrypted) storage client for the subpool-map bucket.
 * @param kmsClient KMS client able to unwrap the EDP's KEK.
 */
class SubpoolFingerprintsStore(storageClient: StorageClient, kmsClient: KmsClient) :
  EncryptedRecordIoStore(storageClient, kmsClient) {
  /** One shard's encrypted blob for a subpool, used as a merge input. */
  data class SubpoolBlob(val blobKey: String, val encryptedDek: EncryptedDek)

  /**
   * Writes [fingerprintChunks] for [poolOffset] to [blobKey], one RecordIO `SubpoolFingerprints`
   * record per emitted chunk (each chunk a multiple of 12 packed bytes), envelope-encrypted with
   * [encryptedDek]. The chunk stream is consumed lazily, so memory stays bounded.
   *
   * TODO(world-federation-of-advertisers/cross-media-measurement#3999): write with an
   *   `ifGenerationMatch=0` (write-if-absent) precondition so a `WorkItem` delivered to two VMs
   *   (Pub/Sub at-least-once) can't last-writer-wins clobber each other's ciphertext — the etag CAS
   *   on `MarkPoolAssignmentJobSucceeded` only guards the row, after the bytes are already on GCS.
   *   The write-if-absent primitive lands in world-federation-of-advertisers/common-jvm#389
   *   (`writeBlobIfGeneration` / `writeBlobIfAbsent` on `ConditionalOperationStorageClient`,
   *   forwarded end-to-end through the `AeadStorageClient` / `StreamingAeadStorageClient` /
   *   `MesosRecordIoStorageClient` wrappers). Once it merges and the common-jvm override is bumped,
   *   collapse the per-attempt-UUID blob keys to deterministic keys written via a read-gen-at-start
   *     + write-with-precondition call, and surface the `412` (without retrying on it) so
   *       [SubpoolAssigner] can take the lost-race path. Not yet available — written
   *       unconditionally for now.
   */
  suspend fun writeBlob(
    blobKey: String,
    encryptedDek: EncryptedDek,
    poolOffset: Long,
    fingerprintChunks: Flow<ByteString>,
  ) {
    encryptedClient(encryptedDek)
      .writeBlob(
        blobKey,
        fingerprintChunks.map { chunk ->
          subpoolFingerprints {
              this.poolOffset = poolOffset
              fingerprints = chunk
            }
            .toByteString()
        },
      )
  }

  /** Streams the `SubpoolFingerprints` records of [blobKey], decrypting with [encryptedDek]. */
  fun readBlob(blobKey: String, encryptedDek: EncryptedDek): Flow<SubpoolFingerprints> = flow {
    val blob = encryptedClient(encryptedDek).getBlob(blobKey) ?: return@flow
    blob.read().collect { record -> emit(SubpoolFingerprints.parseFrom(record)) }
  }

  /**
   * Streams every record from each [inputs] blob into a single merged blob at [outputKey],
   * re-encrypted with [outputEncryptedDek]. Because shards partition the fingerprint space, the
   * inputs are disjoint and merging is a pure concatenation (no dedup). Missing inputs (a shard
   * with no blob for this subpool) are skipped. Memory stays bounded — records stream straight
   * through.
   *
   * TODO(world-federation-of-advertisers/cross-media-measurement#3999): write [outputKey] with an
   *   `ifGenerationMatch=0` (write-if-absent) precondition (see [writeBlob]). Note the merged blob
   *   differs from the per-shard case: [SubpoolAssigner]'s last-shard-out recovery deliberately
   *   re-runs the merge on redelivery, so a `412` here means "already merged" and the caller should
   *   skip-and-continue the idempotent fan-out, not treat it as a lost race.
   */
  suspend fun mergeSubpool(
    inputs: List<SubpoolBlob>,
    outputKey: String,
    outputEncryptedDek: EncryptedDek,
  ) {
    encryptedClient(outputEncryptedDek)
      .writeBlob(
        outputKey,
        flow {
          for (input in inputs) {
            val blob = encryptedClient(input.encryptedDek).getBlob(input.blobKey) ?: continue
            blob.read().collect { record -> emit(record) }
          }
        },
      )
  }

  /** Deletes [blobKey] if present (used to clean up temp per-shard blobs after a merge). */
  suspend fun delete(blobKey: String) {
    storageClient.getBlob(blobKey)?.delete()
  }

  companion object {
    /**
     * Storage key for one shard's `SubpoolFingerprints` blob of [poolOffset], scoped to the
     * (upload, model line) run. Used by the Phase-0 writer and merge.
     */
    fun shardSubpoolKey(
      blobPrefix: String,
      rawImpressionUpload: String,
      modelLine: String,
      shardIndex: Int,
      poolOffset: Long,
    ): String =
      "${runPrefix(blobPrefix, rawImpressionUpload, modelLine)}/shard/$shardIndex" +
        "/subpoolOffset/$poolOffset"

    /**
     * Storage key for the merged (un-sharded) `SubpoolFingerprints` blob of [poolOffset]. Written
     * by the Phase-0 last-shard-out and read by Phase-1; both must use this function so the layout
     * has a single source of truth.
     */
    fun mergedSubpoolKey(
      blobPrefix: String,
      rawImpressionUpload: String,
      modelLine: String,
      poolOffset: Long,
    ): String =
      "${runPrefix(blobPrefix, rawImpressionUpload, modelLine)}/merged/subpoolOffset/$poolOffset"

    /**
     * Per-run key prefix scoping blobs by (upload, model line), so concurrent model-line runs and
     * later uploads under the same static [blobPrefix] never collide. Uses the resource id (last
     * path segment) of each: the upload id is globally unique, and model lines within an upload
     * have distinct names.
     */
    private fun runPrefix(
      blobPrefix: String,
      rawImpressionUpload: String,
      modelLine: String,
    ): String {
      val root = blobPrefix.trimEnd('/')
      val uploadId = rawImpressionUpload.substringAfterLast('/')
      val modelLineId = modelLine.substringAfterLast('/')
      return "$root/upload/$uploadId/modelLine/$modelLineId"
    }
  }
}
