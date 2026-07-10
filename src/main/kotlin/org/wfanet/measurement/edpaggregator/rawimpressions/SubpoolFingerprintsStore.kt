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
import org.wfanet.measurement.storage.ConditionalOperationStorageClient

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
class SubpoolFingerprintsStore(
  storageClient: ConditionalOperationStorageClient,
  kmsClient: KmsClient,
) : EncryptedRecordIoStore(storageClient, kmsClient) {
  /** One shard's encrypted blob for a subpool, used as a merge input. */
  data class SubpoolBlob(val blobKey: String, val encryptedDek: EncryptedDek)

  /**
   * Writes [fingerprintChunks] for [poolOffset] to [blobKey], one RecordIO `SubpoolFingerprints`
   * record per emitted chunk (each chunk a multiple of 12 packed bytes), envelope-encrypted with
   * [encryptedDek]. The chunk stream is consumed lazily, so memory stays bounded.
   *
   * NOTE(world-federation-of-advertisers/cross-media-measurement#3999): this write is intentionally
   * unconditional. common-jvm#389's write-if-absent primitive (`writeBlobIfNotFound`) IS available
   * now, but adopting it on this deterministic key would be UNSAFE for these blobs: each is
   * envelope-encrypted with a per-attempt random DEK (+ AES-GCM random IV), so no retry ever
   * reproduces the bytes. A writer that crashes after this write but before
   * `MarkPoolAssignmentJobSucceeded` is auto-recovered today (the retry re-writes, then marks);
   * with write-if-absent the orphan ciphertext would fail every retry with a 412 and, since the job
   * never reaches SUCCEEDED, strand the shard permanently (DLQ -> FAILED). The clobber it would
   * prevent — a `WorkItem` delivered to two live VMs whose distinct DEKs desync from the
   * mark-winner's row DEK — is real but narrow. A fix that avoids BOTH needs a retry to recover the
   * winner's DEK: row-first DEK persistence, or per-attempt-UUID keys (as [RankIndexStore] uses)
   * with the winning key stored on the `PoolAssignmentJob` row — i.e. an API change. Left
   * unconditional deliberately.
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
   * NOTE(world-federation-of-advertisers/cross-media-measurement#3999): also written
   * unconditionally, for the same reason as [writeBlob]. The merge is deliberately idempotent —
   * [SubpoolAssigner]'s last-shard-out recovery re-runs it on redelivery, and re-writing the merged
   * blob is how that recovery works — so a write-if-absent precondition here would break the very
   * recovery it is meant to support.
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
