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
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexMap
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.StorageClient

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
class RankIndexStore(storageClient: StorageClient, kmsClient: KmsClient) :
  EncryptedRecordIoStore(storageClient, kmsClient) {
  /**
   * Writes [records] to [blobKey], one RecordIO record per emitted [RankIndexMap], envelope-
   * encrypted with [encryptedDek]. The record stream is consumed lazily, so memory stays bounded.
   *
   * @return the SHA-256 of the plaintext payload (concatenated record bytes, in order), for the
   *   `RankIndexBlob.blob_checksum` corruption guard.
   *
   * TODO(world-federation-of-advertisers/cross-media-measurement#3999): once
   *   world-federation-of-advertisers/common-jvm#389 lands, collapse the per-attempt-UUID blob keys
   *   (see [snapshotKey] / [dayOnlyKey]) to deterministic keys written via `writeBlobIfGeneration`
   *   so concurrent writers race-fail at the precondition rather than relying on the row insert as
   *   the linearization point. Mirrors the Phase-0 adoption path in
   *   [SubpoolFingerprintsStore.writeBlob].
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
  ): Flow<RankIndexMap> = flow {
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

  /** Deletes [blobKey] from Cloud Storage if present (used to evict aged-out rank-index blobs). */
  suspend fun delete(blobKey: String) {
    storageClient.getBlob(blobKey)?.delete()
  }

  companion object {
    /**
     * Storage key for a cumulative `SNAPSHOT` blob of [poolOffset], scoped to the (upload, model
     * line) run and to a single write [attemptId].
     *
     * The [attemptId] (a per-write UUID) makes every attempt's key unique, so a concurrent or
     * re-delivered ranker never overwrites another attempt's bytes. The authoritative blob is
     * whichever the winning `RankIndexBlob` row points to (its `blob_uri`); readers always resolve
     * the key from that row, never by recomputing it.
     *
     * TODO(world-federation-of-advertisers/common-jvm#389): once `writeBlobIfGeneration` is
     *   available through the AeadStorageClient / StreamingAeadStorageClient /
     *   MesosRecordIoStorageClient stack, drop [attemptId] and use a deterministic key, guarding
     *   the write with a GCS generation precondition (`expectedGen = priorBlob?.generation ?: 0L`,
     *   read before compute). Keep the blob-first-then-row order; on a 412 (BlobChangedException)
     *   confirm the winning row and ack rather than re-reading the generation and retrying — a
     *   retry would clobber the winner's already-acked bytes and strand its row's DEK reference.
     *   The same collapse applies to [SubpoolFingerprintsStore].
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
