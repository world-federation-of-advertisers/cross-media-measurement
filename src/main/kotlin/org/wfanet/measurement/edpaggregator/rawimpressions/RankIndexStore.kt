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
import java.security.MessageDigest
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexMap
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
import org.wfanet.measurement.storage.MesosRecordIoStorageClient

/**
 * Reads and writes the per-subpool rank-index maps (the Phase-1 `RankIndexMap` blobs) as
 * **RecordIO** blobs, envelope-encrypted with a per-writer DEK.
 *
 * The Phase-1 (`VidRankBuilder`) analog of [SubpoolFingerprintsStore]: Phase-1 reads the prior
 * cumulative `SNAPSHOT` blob and writes the new `SNAPSHOT` + `DAY_ONLY` blobs. Both stores share
 * the DEK-generation and encrypted-client plumbing via [EncryptedRecordIoStore].
 *
 * ## Why RecordIO (and not a single `RankIndexMap` message)
 *
 * A cumulative `SNAPSHOT` for the India worst case is ~1.5 B `(fingerprint, rank)` entries — far
 * past a protobuf message / Java `ByteString`'s ~2 GB cap. So a blob is a **stream of
 * `RankIndexMap` records** via [MesosRecordIoStorageClient]: each record carries `pool_offset`,
 * `ranked_size`, and a chunk of packed fingerprints + ranks + last_seen. Memory stays bounded.
 *
 * ## Integrity
 *
 * [writeBlob] returns the SHA-256 of the plaintext payload (the concatenation of the serialized
 * `RankIndexMap` records, in write order). The caller persists it on the `RankIndexBlob` row
 * (`blob_checksum`); [readBlob] re-derives it over the same record byte sequence and, when an
 * expected checksum is supplied, throws on mismatch so a corrupted blob is detected on load.
 *
 * @param storageClient the base (unencrypted) storage client for the vid-rank-map bucket.
 * @param kmsClient KMS client able to wrap/unwrap the EDP's KEK.
 */
class RankIndexStore(storageClient: ConditionalOperationStorageClient, kmsClient: KmsClient) :
  EncryptedRecordIoStore(storageClient, kmsClient) {
  /**
   * Writes [records] to [blobKey], one RecordIO record per emitted [RankIndexMap], envelope-
   * encrypted with [encryptedDek]. The record stream is consumed lazily, so memory stays bounded.
   *
   * @return the SHA-256 of the plaintext payload (concatenated record bytes, in order), for the
   *   `RankIndexBlob.blob_checksum` corruption guard.
   *
   * NOTE(world-federation-of-advertisers/cross-media-measurement#3999): this write is intentionally
   * unconditional to a per-attempt-UUID key (see [snapshotKey]). Do NOT collapse to a deterministic
   * key + write-if-absent (common-jvm#389 `writeBlobIfNotFound`): the current UUID-key design is
   * already clobber-safe (each attempt writes its own key; the winning `RankIndexBlob` row picks
   * the authoritative blob and a loser just orphans bytes), whereas write-if-absent on a
   * deterministic, envelope-encrypted blob (per-attempt random DEK + IV -> non-reproducible bytes)
   * would REGRESS it: a writer that crashes between the write and the row insert leaves an orphan
   * that fails every retry's 412 forever. See [snapshotKey] and
   * [SubpoolFingerprintsStore.writeBlob].
   */
  suspend fun writeBlob(
    blobKey: String,
    encryptedDek: EncryptedDek,
    records: Flow<RankIndexMap>,
  ): ByteString {
    val digest = MessageDigest.getInstance("SHA-256")
    encryptedClient(encryptedDek)
      .writeBlob(
        blobKey,
        records.map { record ->
          val bytes = record.toByteString()
          digest.update(bytes.asReadOnlyByteBuffer())
          bytes
        },
      )
    return ByteString.copyFrom(digest.digest())
  }

  /**
   * Streams the `RankIndexMap` records of [blobKey], decrypting with [encryptedDek]. When
   * [expectedChecksum] is non-null and non-empty, the plaintext payload is hashed as it streams and
   * an [IllegalStateException] is thrown at end-of-stream on mismatch (corruption guard).
   */
  fun readBlob(
    blobKey: String,
    encryptedDek: EncryptedDek,
    expectedChecksum: ByteString? = null,
    recordBufferCapacity: Int = DEFAULT_READ_RECORD_BUFFER,
  ): Flow<RankIndexMap> =
    flow {
        val blob = encryptedClient(encryptedDek).getBlob(blobKey) ?: return@flow
        val digest =
          if (expectedChecksum != null && !expectedChecksum.isEmpty) {
            MessageDigest.getInstance("SHA-256")
          } else {
            null
          }
        blob.read().collect { record ->
          digest?.update(record.asReadOnlyByteBuffer())
          emit(RankIndexMap.parseFrom(record))
        }
        if (digest != null) {
          val actual = ByteString.copyFrom(digest.digest())
          check(actual == expectedChecksum) {
            "RankIndexBlob $blobKey checksum mismatch; cumulative blob is corrupt"
          }
        }
      }
      .buffer(recordBufferCapacity)

  /** Deletes [blobKey] from Cloud Storage if present (used to evict aged-out rank-index blobs). */
  suspend fun delete(blobKey: String) {
    storageClient.getBlob(blobKey)?.delete()
  }

  /**
   * Byte size of the SNAPSHOT blob at [blobKey], or 0 if absent. The Phase-2 index load uses this
   * to pre-size its per-subpool map: on disk each entry is ~[ON_DISK_BYTES_PER_ENTRY] bytes
   * (12-byte fingerprint + 4-byte fixed32 rank + 2-byte last-seen day), so the entry count is
   * approximately `size / ON_DISK_BYTES_PER_ENTRY` (a safe slight over-estimate given
   * framing/encryption).
   */
  suspend fun blobSize(blobKey: String, encryptedDek: EncryptedDek): Long =
    encryptedClient(encryptedDek).getBlob(blobKey)?.size ?: 0L

  companion object {
    /**
     * Approximate on-disk bytes per rank-index entry: 12-byte fingerprint + 4-byte fixed32 rank +
     * 2-byte last-seen day. Used to pre-size the Phase-2 load map from the blob byte size.
     */
    const val ON_DISK_BYTES_PER_ENTRY = 18L

    /**
     * Prefetch depth (in RankIndexMap records, ~18 MiB each at 1M entries/record) between the
     * read/decrypt/parse producer and the map-insert consumer, so the GCS read runs ahead of
     * inserts instead of stalling on them. Small on purpose: memory ~= capacity x ~18 MiB x
     * concurrent readers (Phase-2 loads up to 6 subpools at once => ~430 MiB total at 4).
     */
    const val DEFAULT_READ_RECORD_BUFFER = 4

    /**
     * Storage key for a cumulative `SNAPSHOT` blob of [poolOffset], scoped to the (upload, model
     * line) run and to a single write [attemptId].
     *
     * The [attemptId] (a per-write UUID) makes every attempt's key unique, so a concurrent or
     * re-delivered ranker never overwrites another attempt's bytes. The authoritative blob is
     * whichever the winning `RankIndexBlob` row points to (its `blob_uri`); readers always resolve
     * the key from that row, never by recomputing it.
     *
     * NOTE(world-federation-of-advertisers/cross-media-measurement#3999): [attemptId] is kept
     * deliberately. The tempting simplification — drop it, use a deterministic key, guard the write
     * with write-if-absent (common-jvm#389 is now available) — would REGRESS crash-recovery: these
     * blobs are envelope-encrypted with a per-attempt random DEK (+ AES-GCM IV), so no retry
     * reproduces the bytes, and a writer that crashes after the write but before the
     * `RankIndexBlob` row insert leaves an orphan that fails every retry with a 412, stranding the
     * subpool. The UUID-key + row-as-linearization design here avoids both that and the clobber. A
     * deterministic-key fix would need the winner's DEK recoverable by a retry (row-first DEK) — an
     * API change.
     */
    fun snapshotKey(
      blobPrefix: String,
      rawImpressionUpload: String,
      modelLine: String,
      poolOffset: Long,
      attemptId: String,
    ): String =
      "${runPrefix(blobPrefix, rawImpressionUpload, modelLine)}/snapshot/subpoolOffset/" +
        "$poolOffset/attempt/$attemptId"

    /**
     * Storage key for a `DAY_ONLY` blob of [poolOffset], scoped to the (upload, model line) run and
     * to a single write [attemptId] (see [snapshotKey]).
     */
    fun dayOnlyKey(
      blobPrefix: String,
      rawImpressionUpload: String,
      modelLine: String,
      poolOffset: Long,
      attemptId: String,
    ): String =
      "${runPrefix(blobPrefix, rawImpressionUpload, modelLine)}/dayOnly/subpoolOffset/" +
        "$poolOffset/attempt/$attemptId"

    /**
     * Per-run key prefix scoping blobs by (upload, model line). Uses the resource id (last path
     * segment) of each: the upload id is globally unique, and model lines within an upload have
     * distinct names. Mirrors [SubpoolFingerprintsStore]'s layout.
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
