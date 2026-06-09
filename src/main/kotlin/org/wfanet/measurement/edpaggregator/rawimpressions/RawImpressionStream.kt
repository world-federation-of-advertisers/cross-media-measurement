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
import java.time.Instant
import java.util.Base64
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import org.apache.parquet.crypto.FileDecryptionProperties
import org.wfanet.measurement.storage.BlobUri
import org.wfanet.measurement.storage.ParquetStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.StorageClient

/**
 * Streams the local VM's shard of raw impressions for one upload,
 * window-filtered and fingerprinted.
 *
 * Each emission is a batch of [FingerprintedEvent]. For every parquet
 * blob in the upload folder identified by [doneBlobPath]:
 *  1. Open via [ParquetStorageClient]; column data is PME-decrypted
 *     page-by-page inside parquet-mr.
 *  2. Per row: drop if outside `[activeStartTime, activeEndTime)`;
 *     compute SHA-256 fingerprint of the event-id column; drop if not
 *     this shard (`fp.high % totalShards != shardIndex`).
 *  3. Emit surviving rows in batches of [batchSize].
 *
 * ## Blob discovery
 *
 * [doneBlobPath] points at the `done` marker an EDP writes at the end
 * of an upload (mirrors `VidLabelingDispatcher.dispatch`). The stream
 * lists the containing folder and treats every blob except the marker
 * as a raw-impression parquet file.
 *
 * ## Encryption
 *
 * Per-blob PME bootstrap via this class's [resolveBlobDecryption]
 * callback wired into [ParquetStorageClient]: read `kek_uri` +
 * `encrypted_dek` from the plaintext footer, unwrap via
 * `kmsClient.getAead(kekUri).decrypt(...)`, hand raw AES bytes to
 * parquet-mr. Plaintext column data never leaves the JVM heap; the
 * on-disk temp file stays PME-encrypted.
 *
 * ## Column mapping
 *
 * [labelerInputFieldMapping] mirrors
 * `VidLabelingConfig.ModelLineConfig.labeler_input_field_mapping`
 * (key = LabelerInput field path, value = parquet column name). Two
 * entries are required:
 *  - `timestamp_usec` -> parquet INT64 column carrying event time in
 *    epoch microseconds.
 *  - `event_id.id` -> parquet BINARY/STRING column carrying the event
 *    identifier bytes.
 *
 * Every other field stays in [FingerprintedEvent.row] for downstream
 * consumers to project as they see fit.
 *
 * ## Parallelism
 *
 *  - **Producer**: up to [ioParallelism] per-blob coroutines run
 *    concurrently on `Dispatchers.IO.limitedParallelism(...)`. Each
 *    drops out-of-window and out-of-shard rows BEFORE emission, so
 *    ~`(N-1)/N` of input rows never reach the output channel.
 *  - **Channel** is bounded by [bufferedBatches] to backpressure
 *    producers when consumers fall behind.
 *  - **Consumer** parallelism is the caller's responsibility. The
 *    returned `Flow` is cold and sequential at the collection point;
 *    CPU-bound downstream work should fan out via
 *    `flatMapMerge(workers)`.
 */
class RawImpressionStream(
  private val storageClient: StorageClient,
  private val kmsClient: KmsClient,
  private val doneBlobPath: String,
  private val labelerInputFieldMapping: Map<String, String>,
  private val activeStartTime: Instant,
  private val activeEndTime: Instant?,
  private val shardIndex: Int,
  private val totalShards: Int,
  private val fingerprintExtractor: FingerprintExtractor,
  // Number of survivors accumulated per emitted batch. Mirrors
  // StorageEventReader's production default of 256.
  private val batchSize: Int = DEFAULT_BATCH_SIZE,
  // No oversubscription: on n2d-highmem-16 (16 vCPU) this is 16. Each
  // per-blob coroutine does mixed CPU (parquet parse + decrypt +
  // fingerprint) and network work; over-allocating threads here would
  // thrash the CPU during the decrypt-heavy windows.
  private val ioParallelism: Int = Runtime.getRuntime().availableProcessors(),
  // Capacity of the bounded channel between per-blob producers and the
  // downstream collector. Sized to absorb consumer-side jitter without
  // blocking producers; memory footprint stays in the tens of MB
  // (ioParallelism * 2 * batchSize records).
  private val bufferedBatches: Int = ioParallelism * 2,
) {
  /** Atomic counters updated as the cold flow is consumed. */
  data class Stats(
    val read: AtomicLong = AtomicLong(),
    val droppedOutsideWindow: AtomicLong = AtomicLong(),
    val droppedOtherShard: AtomicLong = AtomicLong(),
    val emitted: AtomicLong = AtomicLong(),
  )

  val stats: Stats = Stats()

  // Resolve the parquet column names for the two semantic fields via
  // the well-known LabelerInput paths. Failing here keeps
  // misconfiguration out of the hot path.
  private val eventTimeColumn: String =
    labelerInputFieldMapping[EVENT_TIME_FIELD_PATH]
      ?: throw IllegalArgumentException(
        "labelerInputFieldMapping is missing required entry for " +
          "'$EVENT_TIME_FIELD_PATH'; got keys ${labelerInputFieldMapping.keys}"
      )

  private val eventIdColumn: String =
    labelerInputFieldMapping[EVENT_ID_FIELD_PATH]
      ?: throw IllegalArgumentException(
        "labelerInputFieldMapping is missing required entry for " +
          "'$EVENT_ID_FIELD_PATH'; got keys ${labelerInputFieldMapping.keys}"
      )

  // One ParquetStorageClient is shared across all per-blob reads.
  // PME decryption properties are resolved per-blob from each parquet
  // file's plaintext footer via the callback below.
  private val parquetClient: ParquetStorageClient =
    ParquetStorageClient(storageClient, decryptionPropertiesProvider = ::resolveBlobDecryption)

  init {
    require(totalShards > 0) { "totalShards must be positive, got $totalShards" }
    require(shardIndex in 0 until totalShards) {
      "shardIndex ($shardIndex) must be in [0, totalShards=$totalShards)"
    }
    require(batchSize > 0) { "batchSize must be positive, got $batchSize" }
    require(ioParallelism > 0) { "ioParallelism must be positive, got $ioParallelism" }
    require(bufferedBatches > 0) { "bufferedBatches must be positive, got $bufferedBatches" }
    require(doneBlobPath.isNotBlank()) { "doneBlobPath must be non-blank" }
    activeEndTime?.let {
      require(it.isAfter(activeStartTime)) {
        "activeEndTime ($it) must be strictly after activeStartTime ($activeStartTime)"
      }
    }
  }

  /** See class-level KDoc for parallelism contract. */
  fun stream(): Flow<List<FingerprintedEvent>> {
    val ioDispatcher = Dispatchers.IO.limitedParallelism(ioParallelism)
    return channelFlow {
        val blobUris = discoverBlobUris()
        blobUris.forEach { blobUri ->
          launch(ioDispatcher) { streamBlob(blobUri) { batch -> send(batch) } }
        }
      }
      .buffer(bufferedBatches)
  }

  /** Inclusive lower bound, exclusive upper bound; `null` end means open-ended. */
  internal fun isInsideActiveWindow(eventTime: Instant): Boolean {
    if (eventTime.isBefore(activeStartTime)) return false
    val end = activeEndTime ?: return true
    return eventTime.isBefore(end)
  }

  internal fun belongsToShard(fingerprint: Fingerprint): Boolean {
    // SHA-256's avalanche makes the upper Long a uniform shard hash on
    // its own — no mixing with the low 4 bytes needed.
    val shard = Math.floorMod(fingerprint.high, totalShards.toLong()).toInt()
    return shard == shardIndex
  }

  /**
   * Parses [doneBlobPath], lists its folder, drops the done marker,
   * and rebuilds full blob URIs for the remaining files. Mirrors
   * `VidLabelingDispatcher.dispatch`.
   */
  private suspend fun discoverBlobUris(): List<String> {
    val doneBlobUri: BlobUri = SelectedStorageClient.parseBlobUri(doneBlobPath)
    val folderPrefix: String = doneBlobUri.key.substringBeforeLast("/")
    return storageClient
      .listBlobs(folderPrefix)
      .toList()
      .asSequence()
      .map { it.blobKey }
      .filterNot { isDoneMarker(it) }
      .map { buildBlobUri(doneBlobUri, it) }
      .toList()
  }

  private fun isDoneMarker(blobKey: String): Boolean =
    blobKey.substringAfterLast("/").equals(DONE_MARKER_FILE_NAME, ignoreCase = true)

  private fun buildBlobUri(doneBlobUri: BlobUri, blobKey: String): String =
    when (doneBlobUri.scheme) {
      "gs" -> "${doneBlobUri.scheme}://${doneBlobUri.bucket}/$blobKey"
      "file" -> "${doneBlobUri.scheme}:///${doneBlobUri.bucket}/$blobKey"
      else -> throw IllegalArgumentException("Unsupported scheme: ${doneBlobUri.scheme}")
    }

  /**
   * Per-blob PME bootstrap callback. Invoked at most once per blob by
   * [ParquetStorageClient] when the first row is read. Reads the
   * blob's plaintext parquet footer, pulls `kek_uri` + `encrypted_dek`,
   * unwraps the DEK via KMS, and returns parquet's
   * [FileDecryptionProperties] keyed by the raw AES bytes.
   *
   * Re-entry note: this callback MAY call [ParquetBlob.readKeyValueMetadata]
   * on the same blob (that's the bootstrap) but MUST NOT call
   * [ParquetBlob.readRows] — see common-jvm `ParquetStorageClient` KDoc.
   */
  private suspend fun resolveBlobDecryption(
    blob: ParquetStorageClient.ParquetBlob
  ): FileDecryptionProperties {
    val md = blob.readKeyValueMetadata()
    val kekUri =
      md[FOOTER_KEY_KEK_URI]
        ?: error("Parquet footer of '${blob.blobKey}' missing '$FOOTER_KEY_KEK_URI' entry")
    val encryptedDekB64 =
      md[FOOTER_KEY_ENCRYPTED_DEK]
        ?: error("Parquet footer of '${blob.blobKey}' missing '$FOOTER_KEY_ENCRYPTED_DEK' entry")
    val encryptedDekBytes: ByteArray =
      try {
        Base64.getDecoder().decode(encryptedDekB64)
      } catch (e: IllegalArgumentException) {
        throw IllegalStateException(
          "Parquet footer entry '$FOOTER_KEY_ENCRYPTED_DEK' of '${blob.blobKey}' is not " +
            "valid base64",
          e,
        )
      }
    val rawAesKey: ByteArray =
      kmsClient.getAead(kekUri).decrypt(encryptedDekBytes, EMPTY_AAD)
    return FileDecryptionProperties.builder().withFooterKey(rawAesKey).build()
  }

  private suspend fun streamBlob(
    blobUri: String,
    emit: suspend (List<FingerprintedEvent>) -> Unit,
  ) {
    val blobKey = SelectedStorageClient.parseBlobUri(blobUri).key
    val parquetBlob =
      parquetClient.getBlob(blobKey)
        ?: error("Raw-impression blob not found: $blobUri")

    parquetBlob.use { blob ->
      val current = ArrayList<FingerprintedEvent>(batchSize)
      blob.readRows().collect { row ->
        stats.read.incrementAndGet()

        // Window filter — runs BEFORE the per-row fingerprint cost.
        val eventTime = epochMicrosToInstant(row[eventTimeColumn] as Long)
        if (!isInsideActiveWindow(eventTime)) {
          stats.droppedOutsideWindow.incrementAndGet()
          return@collect
        }

        // Shard filter — fingerprint is unavoidable here.
        val idBytes: ByteString = readEventIdBytes(row, blobUri)
        val fingerprint = fingerprintExtractor.extract(idBytes)
        if (!belongsToShard(fingerprint)) {
          stats.droppedOtherShard.incrementAndGet()
          return@collect
        }

        current.add(FingerprintedEvent(row, fingerprint))
        if (current.size >= batchSize) {
          emit(current.toList())
          stats.emitted.addAndGet(current.size.toLong())
          current.clear()
        }
      }
      // Trailing partial batch.
      if (current.isNotEmpty()) {
        emit(current.toList())
        stats.emitted.addAndGet(current.size.toLong())
      }
    }
  }

  /**
   * Reads the event-id column from [row] as bytes. The column is
   * declared `LabelerInput.event_id.id` which is `string`; parquet may
   * surface it either as a Kotlin [String] (BINARY with `STRING`
   * logical type) or as a raw [ByteString] (BINARY without
   * annotation). We accept either.
   */
  private fun readEventIdBytes(row: Map<String, Any?>, blobUri: String): ByteString =
    when (val v = row[eventIdColumn]) {
      is String -> ByteString.copyFromUtf8(v)
      is ByteString -> v
      else ->
        error(
          "event-id column '$eventIdColumn' of '$blobUri' has unexpected type " +
            "${v?.javaClass?.simpleName} (expected String or ByteString)"
        )
    }

  /** Converts an epoch-microseconds `long` to an [Instant]. */
  private fun epochMicrosToInstant(epochMicros: Long): Instant {
    val seconds = Math.floorDiv(epochMicros, 1_000_000L)
    val nanos = Math.floorMod(epochMicros, 1_000_000L) * 1_000L
    return Instant.ofEpochSecond(seconds, nanos)
  }

  companion object {
    const val DEFAULT_BATCH_SIZE = 256

    /** Marker file name written at the end of an upload. */
    private const val DONE_MARKER_FILE_NAME = "done"

    /** Well-known LabelerInput field path for event timestamp (epoch micros). */
    private const val EVENT_TIME_FIELD_PATH = "timestamp_usec"

    /** Well-known LabelerInput field path for the publisher-specific event id. */
    private const val EVENT_ID_FIELD_PATH = "event_id.id"

    /** Parquet footer key carrying the KMS KEK URI used to wrap the DEK. */
    private const val FOOTER_KEY_KEK_URI = "kek_uri"

    /**
     * Parquet footer key carrying the base64-encoded ciphertext of the
     * raw AES key (encrypted directly by the KEK Aead, NOT a Tink
     * keyset, NOT a serialized proto envelope).
     */
    private const val FOOTER_KEY_ENCRYPTED_DEK = "encrypted_dek"

    /**
     * Associated Data used during KMS `Aead.encrypt` of the DEK on
     * the writer side. Must match what the EDP used when wrapping.
     * Empty by default.
     */
    private val EMPTY_AAD = ByteArray(0)
  }
}
