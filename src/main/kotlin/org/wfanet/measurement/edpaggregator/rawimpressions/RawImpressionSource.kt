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

import com.google.protobuf.ByteString
import io.grpc.StatusException
import java.util.concurrent.atomic.AtomicInteger
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext
import kotlin.time.TimeSource
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withContext
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadFilesRequest
import org.wfanet.measurement.storage.ParquetStorageClient
import org.wfanet.measurement.storage.ParquetValue

/**
 * Reads the local VM's shard of raw impressions for one upload in parallel and
 * hands shard-filtered batches to a caller-supplied per-file [BlobSink]. The class
 * owns the **parallel reading skeleton** (discovery, decode/decrypt, event-id
 * digest, shard filter, batching, bounded concurrency, backpressure, per-blob
 * lifecycle) and is **agnostic to what downstream does** with the events —
 * labeling, pool assignment, etc. all live in the [BlobSink].
 *
 * ## Execution model (two stages joined by a bounded channel)
 *
 * 1. **Unwrappers** — one coroutine per open input file, on [readDispatcher] (an
 *    *elastic* dispatcher, [Dispatchers.IO] by default), bounded to [maxOpenFiles]
 *    concurrent files by a [Semaphore]. Each reads its file's rows (parquet decode
 *    + PME AES-GCM decrypt + decompression), computes the event-id [EventIdDigest],
 *    shard-filters, batches the survivors, and sends each batch onto the shared
 *    [workerChannel][Channel].
 * 2. **Workers** — a fixed pool of [workers] coroutines on [workerDispatcher]
 *    ([Dispatchers.Default] ≈ #cores by default). Each pulls an [EventBatch] off
 *    the shared channel (work-stealing: any idle worker takes the next batch) and
 *    calls [BlobSink.processBatch] on that batch's blob's sink. A single large file
 *    is processed by the whole pool in parallel, and a thousand small files too.
 *
 * **Backpressure lives here:** the worker channel is bounded ([workerChannelCapacity]);
 * when the workers fall behind, `send` suspends the unwrappers, throttling file
 * reads to downstream consumption speed.
 *
 * ## Per-blob lifecycle & concurrency
 *
 * Because batches from all files are mixed in one shared pool, a batch is decoupled
 * from its file, so per-file completion is tracked by hand via [BlobContext]: an
 * `AtomicInteger` (starting at 1 for "reading in progress") + a [CompletableDeferred];
 * the unwrapper reserves one before each send and releases the reservation when
 * reading finishes, while a worker releases one per batch processed. When the count
 * hits 0 the file is done. Each [EventBatch] is tagged with its [BlobContext].
 *
 * A fresh sink is minted **per blob** by `openSink(blobUri)`, because up to
 * [maxOpenFiles] files are processed concurrently. **[BlobSink.processBatch] is
 * called concurrently for the same blob** (the shared pool), so it MUST be
 * thread-safe or serialize internally. [BlobSink.commit] runs once **on success**
 * (finalize/publish) and [BlobSink.close] **always** runs once to release resources
 * — even on cancellation, so a cancelled blob never leaks.
 *
 * ## Filtering (done here)
 *
 * Only the **shard filter** is applied in the unwrapper, so non-shard rows never
 * reach the worker channel: `floorMod(digest.high, totalShards) == shardIndex`. It
 * is digest-only and identical for every model line in a WorkItem, so it is paid
 * once. (SHA-256 avalanche makes `digest.high` a uniform shard hash.)
 *
 * ## What this class does NOT do
 *
 * Time-window filtering, labeling, model-line fan-out, and output writing are all
 * the [BlobSink]'s job. Window filtering in particular is **per model line**, so
 * the sink does it with the shared `ActiveWindow` utility (reading the event time
 * from [DigestedEvent.row]); this class is window-agnostic.
 *
 * ## Blob discovery
 *
 * Input files are discovered by listing the [rawImpressionUpload]'s
 * `RawImpressionUploadFile`s via [rawImpressionUploadFilesStub]
 * (`ListRawImpressionUploadFiles`, paginated). Each file's `blob_uri` is a full
 * Cloud Storage URI handed straight to [parquetStorageClient].
 *
 * ## Encryption
 *
 * PME decryption is handled declaratively by the injected [parquetStorageClient].
 * Plaintext column data never leaves the JVM heap.
 *
 * ## Failure semantics
 *
 * If reading a file or a [BlobSink.processBatch] call throws, the exception
 * propagates, the enclosing [coroutineScope] cancels every sibling (unwrappers and
 * workers), and the **failed** blob's `sink.commit()` is skipped → it publishes no
 * partial output; its [BlobSink.close] still runs (under [NonCancellable]) to
 * release resources. This is a **per-blob, not per-upload** guarantee: blobs that
 * committed before the failure are already published, so a retry must be idempotent
 * per blob.
 *
 * @property rawImpressionUploadFilesStub client used to list the upload's files.
 * @property rawImpressionUpload resource name of the upload to read, format
 *   `dataProviders/{data_provider}/rawImpressionUploads/{raw_impression_upload}`.
 * @property eventIdColumn parquet column holding the event id (used for the
 *   digest). A `STRING` (BINARY+STRING) or raw `BINARY` column; both are accepted.
 */
class RawImpressionSource(
  private val parquetStorageClient: ParquetStorageClient,
  private val rawImpressionUploadFilesStub: RawImpressionUploadFileServiceCoroutineStub,
  private val rawImpressionUpload: String,
  private val eventIdColumn: String,
  private val shardIndex: Int,
  private val totalShards: Int,
  private val eventIdDigestExtractor: EventIdDigestExtractor,
  private val metrics: RawImpressionSourceMetrics = RawImpressionSourceMetrics(),
  private val readDispatcher: CoroutineContext = Dispatchers.IO,
  private val workerDispatcher: CoroutineContext = Dispatchers.Default,
  private val maxOpenFiles: Int = DEFAULT_MAX_OPEN_FILES,
  private val workers: Int = DEFAULT_WORKERS,
  private val batchSize: Int = DEFAULT_BATCH_SIZE,
  private val workerChannelCapacity: Int = DEFAULT_WORKER_CHANNEL_CAPACITY,
) {
  init {
    require(totalShards > 0) { "totalShards must be positive, got $totalShards" }
    require(shardIndex in 0 until totalShards) {
      "shardIndex ($shardIndex) must be in [0, totalShards=$totalShards)"
    }
    require(maxOpenFiles > 0) { "maxOpenFiles must be positive, got $maxOpenFiles" }
    require(workers > 0) { "workers must be positive, got $workers" }
    require(batchSize > 0) { "batchSize must be positive, got $batchSize" }
    require(workerChannelCapacity > 0) {
      "workerChannelCapacity must be positive, got $workerChannelCapacity"
    }
    require(rawImpressionUpload.isNotBlank()) { "rawImpressionUpload must be non-blank" }
    require(eventIdColumn.isNotBlank()) { "eventIdColumn must be non-blank" }
  }

  /**
   * Caller-supplied, per-blob consumer of shard-filtered event batches. Minted per
   * file by the `openSink` lambda passed to [streamBlobs].
   *
   * Threading: [processBatch] is invoked **concurrently by the worker pool** for
   * the same blob, so it MUST be thread-safe or serialize internally. [commit] runs
   * once on success (after every [processBatch] has returned); [close] always runs
   * once to release resources.
   *
   *  - **Phase 2 (labeling):** per-blob output. `processBatch` labels each event
   *    and streams the records to this blob's own output; `commit` finalizes/uploads
   *    it and `close` releases the writer.
   *  - **Phase 0 (pool assignment):** `processBatch` records each event's
   *    `(subpoolId, eventIdDigest)` into a single shared concurrency-safe map;
   *    `commit`/`close` are no-ops and the map is uploaded once after [streamBlobs]
   *    returns.
   */
  interface BlobSink {
    /** Processes one shard-filtered batch for this blob. Called concurrently. */
    suspend fun processBatch(events: List<DigestedEvent>)

    /** Finalizes + publishes this blob's output. Once, only on success. */
    suspend fun commit()

    /** Releases resources. Always, once (idempotent; must not publish partial output). */
    suspend fun close()
  }

  /** A batch of shard-surviving events tagged with the [blob] they came from. */
  private class EventBatch(val events: List<DigestedEvent>, val blob: BlobContext)

  /**
   * Per-input-file coordination: the blob's [sink] plus an outstanding-work counter
   * (a wait-group) that signals [allBatchesDone] once the file is fully read AND
   * every batch it produced has been processed by the worker pool.
   */
  private class BlobContext(val sink: BlobSink) {
    private val outstanding = AtomicInteger(1)
    val allBatchesDone = CompletableDeferred<Unit>()

    fun reserveBatch() {
      outstanding.incrementAndGet()
    }

    fun release() {
      if (outstanding.decrementAndGet() == 0) allBatchesDone.complete(Unit)
    }
  }

  /**
   * Streams this VM's shard of the upload exactly once (unwrappers → worker channel
   * → worker pool) and suspends until every file has been fully read and processed.
   *
   * Delivers **every** row that passes the shard filter — including **duplicate
   * [EventIdDigest]s** (the same event id can appear in many impressions). Callers
   * needing unique digests (e.g. Phase 0's subpool map) de-duplicate in their
   * [BlobSink].
   *
   * @param openSink mints a fresh [BlobSink] for one input file, given its
   *   `blobUri`. Its [BlobSink.processBatch] is invoked concurrently; state shared
   *   across files captured by this lambda must be concurrency-safe.
   */
  suspend fun streamBlobs(openSink: suspend (blobUri: String) -> BlobSink) {
    coroutineScope {
      val workerChannel = Channel<EventBatch>(workerChannelCapacity)

      // Stage 1: unwrappers, ≤ maxOpenFiles open at once. Closes the worker channel
      // once every file is read so the workers terminate after draining.
      launch(readDispatcher) {
        val blobUris = discoverBlobUris()
        logger.info(
          "Starting raw-impression read of ${blobUris.size} file(s) for shard " +
            "$shardIndex/$totalShards of upload $rawImpressionUpload"
        )
        val progress = ProgressTracker(blobUris.size)
        val openFiles = Semaphore(maxOpenFiles)
        coroutineScope {
          for (blobUri in blobUris) {
            launch(readDispatcher) {
              openFiles.withPermit { processBlob(blobUri, openSink, workerChannel, progress) }
            }
          }
        }
        workerChannel.close()
        progress.logSummary()
      }

      // Stage 2: worker pool draining the shared worker channel.
      repeat(workers) {
        launch(workerDispatcher) {
          for (batch in workerChannel) dispatchBatch(batch)
        }
      }
    }
  }

  /**
   * Reads one input file on its own coroutine: mints its [BlobSink], batches its
   * shard-surviving events onto the [workerChannel], and holds the file "open"
   * (keeping its [maxOpenFiles] permit) until every batch has been processed, then
   * commits and always closes.
   */
  private suspend fun processBlob(
    blobUri: String,
    openSink: suspend (blobUri: String) -> BlobSink,
    workerChannel: SendChannel<EventBatch>,
    progress: ProgressTracker,
  ) {
    val startTime: TimeSource.Monotonic.ValueTimeMark = TimeSource.Monotonic.markNow()
    logger.fine { "Reading raw-impression file $blobUri" }
    val blob = BlobContext(openSink(blobUri))
    try {
      var batch = ArrayList<DigestedEvent>(batchSize)
      val flush = suspend {
        if (batch.isNotEmpty()) {
          blob.reserveBatch()
          workerChannel.send(EventBatch(batch, blob))
          batch = ArrayList(batchSize)
        }
      }
      val counts =
        readEventsFromBlob(blobUri) { event ->
          batch.add(event)
          if (batch.size >= batchSize) flush()
        }
      flush()
      // Release the reading reservation; the blob's batches finish on the worker
      // pool, then we finalize. Reached only on success.
      blob.release()
      blob.allBatchesDone.await()
      blob.sink.commit()
      metrics.fileProcessingDurationHistogram.record(
        startTime.elapsedNow().inWholeMilliseconds / 1000.0
      )
      logger.fine {
        "Read ${counts.read} row(s) from $blobUri " +
          "(emitted ${counts.emitted}, dropped ${counts.droppedOtherShard})"
      }
      progress.recordFileComplete(counts)
    } finally {
      // Always release the sink's resources, even on cancellation, so an
      // open-but-uncommitted sink never leaks. close() must not publish output.
      withContext(NonCancellable) { blob.sink.close() }
    }
  }

  /** Hands one [EventBatch] to its blob's [BlobSink]. Runs on the worker pool. */
  private suspend fun dispatchBatch(batch: EventBatch) {
    batch.blob.sink.processBatch(batch.events)
    batch.blob.release()
  }

  internal fun belongsToShard(digest: EventIdDigest): Boolean {
    // SHA-256's avalanche makes the upper Long a uniform shard hash on its own.
    val shard = Math.floorMod(digest.high, totalShards.toLong()).toInt()
    return shard == shardIndex
  }

  /**
   * Reads one blob, computes the event-id digest, applies the shard filter, and
   * invokes [onEvent] inline for every surviving event. Runs on the file's
   * unwrapper coroutine; returns the file's row tallies.
   */
  private suspend fun readEventsFromBlob(
    blobUri: String,
    onEvent: suspend (DigestedEvent) -> Unit,
  ): FileCounts {
    val parquetBlob =
      parquetStorageClient.getBlob(blobUri) ?: error("Raw-impression blob not found: $blobUri")
    var read = 0L
    var droppedOtherShard = 0L
    var emitted = 0L
    parquetBlob.readRows().collect { row ->
      read++
      val digest = eventIdDigestExtractor.extract(readEventIdBytes(row, blobUri))
      if (!belongsToShard(digest)) {
        droppedOtherShard++
        return@collect
      }
      emitted++
      onEvent(DigestedEvent(row, digest))
    }
    // Aggregate per file, then emit to the OTel counters once (instead of per row).
    metrics.rowsReadCounter.add(read)
    metrics.rowsDroppedOtherShardCounter.add(droppedOtherShard)
    metrics.rowsEmittedCounter.add(emitted)
    return FileCounts(read, droppedOtherShard, emitted)
  }

  /**
   * Lists the [rawImpressionUpload]'s `RawImpressionUploadFile`s via the metadata
   * service (paginated) and returns their Cloud Storage `blob_uri`s.
   */
  private suspend fun discoverBlobUris(): List<String> {
    val blobUris = mutableListOf<String>()
    var nextPageToken = ""
    do {
      val response =
        try {
          rawImpressionUploadFilesStub.listRawImpressionUploadFiles(
            listRawImpressionUploadFilesRequest {
              parent = rawImpressionUpload
              pageSize = LIST_PAGE_SIZE
              pageToken = nextPageToken
            }
          )
        } catch (e: StatusException) {
          throw Exception("Error listing RawImpressionUploadFiles for $rawImpressionUpload", e)
        }
      response.rawImpressionUploadFilesList.forEach { blobUris.add(it.blobUri) }
      nextPageToken = response.nextPageToken
    } while (nextPageToken.isNotEmpty())
    return blobUris
  }

  /**
   * Reads the [eventIdColumn] from [row] as bytes. Parquet may surface it either as
   * a `STRING_VALUE` (BINARY+STRING) or a `BYTES_VALUE` (raw BINARY); both accepted.
   */
  private fun readEventIdBytes(row: Map<String, ParquetValue>, blobUri: String): ByteString {
    val v = row.getValue(eventIdColumn)
    return when (v.kindCase) {
      ParquetValue.KindCase.STRING_VALUE -> ByteString.copyFromUtf8(v.stringValue)
      ParquetValue.KindCase.BYTES_VALUE -> v.bytesValue
      else ->
        error(
          "event-id column '$eventIdColumn' of '$blobUri' has unexpected kind " +
            "${v.kindCase} (expected STRING_VALUE or BYTES_VALUE)"
        )
    }
  }

  /** Per-file row tallies returned by [readEventsFromBlob]. */
  private class FileCounts(val read: Long, val droppedOtherShard: Long, val emitted: Long)

  /**
   * Concurrency-safe progress logging across files (which complete on many
   * coroutines): logs a line every ~10% of files completed and a final summary.
   * Keeps its own in-process counts independent of the OTel counters in [metrics].
   */
  private class ProgressTracker(private val totalFiles: Int) {
    private var filesCompleted = 0
    private var totalRead = 0L
    private var totalDropped = 0L
    private var totalEmitted = 0L

    @Synchronized
    fun recordFileComplete(counts: FileCounts) {
      filesCompleted++
      totalRead += counts.read
      totalDropped += counts.droppedOtherShard
      totalEmitted += counts.emitted
      val step = maxOf(1, totalFiles / 10)
      if (filesCompleted % step == 0 || filesCompleted == totalFiles) {
        logger.info(
          "Progress: ${filesCompleted * 100 / totalFiles}% " +
            "($filesCompleted/$totalFiles files) - read=$totalRead " +
            "emitted=$totalEmitted dropped=$totalDropped"
        )
      }
    }

    @Synchronized
    fun logSummary() {
      logger.info(
        "Completed raw-impression read: $filesCompleted/$totalFiles files - " +
          "read=$totalRead emitted=$totalEmitted dropped=$totalDropped"
      )
    }
  }

  companion object {
    private val logger = Logger.getLogger(RawImpressionSource::class.java.name)

    /** ~#cores: the CPU bottleneck for downstream processing. */
    private val DEFAULT_WORKERS: Int = maxOf(1, Runtime.getRuntime().availableProcessors())

    /** ~2× cores: enough open readers to keep the worker channel full despite GCS stalls. */
    private val DEFAULT_MAX_OPEN_FILES: Int =
      maxOf(1, Runtime.getRuntime().availableProcessors() * 2)

    /** Shard-surviving events per batch. */
    private const val DEFAULT_BATCH_SIZE = 256

    /** Worker-channel depth in batches, between unwrappers and the worker pool. */
    private const val DEFAULT_WORKER_CHANNEL_CAPACITY = 64

    /** Page size for listing the upload's files. */
    private const val LIST_PAGE_SIZE = 1000
  }
}
