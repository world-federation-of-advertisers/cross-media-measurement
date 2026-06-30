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
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext
import kotlin.time.TimeSource
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withContext
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.getRawImpressionUploadFileRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadFilesRequest
import org.wfanet.measurement.storage.ParquetStorageClient
import org.wfanet.measurement.storage.ParquetValue

/**
 * A shard-surviving parquet row + its event-id digest, delivered to a
 * [RawImpressionSource.BlobSink].
 */
typealias ParquetDigestedEvent = DigestedEvent<Map<String, ParquetValue>>

/**
 * Reads the local VM's shard of raw impressions for one upload in parallel and hands shard-filtered
 * batches to a caller-supplied per-file [BlobSink]. The class owns the **parallel reading
 * skeleton** (discovery, decode/decrypt, event-id digest, shard filter, batching, bounded
 * concurrency, backpressure, per-blob lifecycle) and is **agnostic to what downstream does** with
 * the events — labeling, pool assignment, etc. all live in the [BlobSink].
 *
 * ## Execution model
 *
 * One coroutine **per open input file** (on [readDispatcher], an elastic dispatcher —
 * [Dispatchers.IO] by default), bounded to [maxOpenFiles] concurrent files by a [Semaphore]. Each
 * file coroutine:
 * 1. opens its [BlobSink];
 * 2. reads the file's rows (parquet decode + PME AES-GCM decrypt + decompression), computes the
 *    event-id [EventIdDigest], shard-filters, and batches survivors;
 * 3. inside a **per-file [coroutineScope]**, launches each batch as a child on the shared
 *    `cpuDispatcher` (= [workerDispatcher] limited to [workers] threads) — so one file's batches
 *    run on many cores at once (intra-file parallelism), and even a lone large file saturates the
 *    cores;
 * 4. the scope **joins all batch children** — so per-file completion is structural (no shared belt,
 *    no per-batch file tagging, no completion latch);
 * 5. then [commits][BlobSink.commit] and always [closes][BlobSink.close] the sink.
 *
 * Why this shape (vs ResultsFulfiller's shared channel + fixed worker pool): output here is **per
 * file**, so a per-file [coroutineScope] that joins its own batch jobs gives completion for free —
 * the single-sink shared-pool shape would force a hand-rolled latch + per-batch tagging that this
 * design avoids.
 *
 * **Global CPU cap (saturation):** every batch from every file runs on the single `cpuDispatcher`
 * capped at [workers], so total CPU parallelism never exceeds the cores even with many files open.
 * This shared dispatcher is the analog of a fixed worker pool: any file's batches fill any free CPU
 * slot (global load-balancing).
 *
 * **Backpressure / memory bound:** a global [Semaphore] of [maxInFlightBatches] permits
 * (`inFlight`) is acquired before each batch is launched and released when it finishes. When that
 * many batches are launched-but-unfinished, a reader's next acquire suspends, pausing its read — so
 * readers run at most [maxInFlightBatches] batches ahead of the CPU workers (the analog of a
 * bounded channel's capacity). Keep [maxInFlightBatches] > [workers] so a finishing worker always
 * has a decoded batch already queued (read-ahead depth = [maxInFlightBatches] − [workers]).
 *
 * ## Per-blob lifecycle & concurrency
 *
 * A fresh sink is minted **per blob** by `openSink(blobUri)` (not per call), because up to
 * [maxOpenFiles] files are processed concurrently, so that many sinks must be live at once. Because
 * the per-file scope launches one batch per `launch(cpuDispatcher)`, **[BlobSink.processBatch] is
 * called concurrently for the same blob** and MUST be thread-safe or serialize internally. After
 * every `processBatch` for the blob has returned (the scope join guarantees this),
 * [BlobSink.commit] runs once **on success** (finalize/publish) and [BlobSink.close] **always**
 * runs once to release resources — even on cancellation, so a cancelled blob never leaks.
 *
 * ## Filtering (done here)
 *
 * Only the **shard filter** is applied in the reader, so non-shard rows never reach the CPU
 * workers: `floorMod(digest.high, totalShards) == shardIndex`. It is digest-only and identical for
 * every model line in a WorkItem, so it is paid once. (SHA-256 avalanche makes `digest.high` a
 * uniform shard hash.)
 *
 * ## What this class does NOT do
 *
 * Time-window filtering, labeling, model-line fan-out, and output writing are all the [BlobSink]'s
 * job. Window filtering in particular is **per model line**, so the sink does it with the shared
 * `ActiveWindow` utility (reading the event time from [DigestedEvent.row]); this class is
 * window-agnostic.
 *
 * ## Blob discovery
 *
 * Input files are discovered by listing the [rawImpressionUpload]'s `RawImpressionUploadFile`s via
 * [rawImpressionUploadFilesStub] (`ListRawImpressionUploadFiles`, paginated). Each file's
 * `blob_uri` is a full Cloud Storage URI handed straight to [parquetStorageClient]
 * ([ParquetStorageClient] resolves an absolute URI to itself, so the client's root is irrelevant
 * for the read).
 *
 * ## Encryption
 *
 * PME decryption is handled declaratively by the injected [parquetStorageClient] (configure it with
 * a `ParquetDecryptionConfig` pointing at the EDP's footer keys). Plaintext column data never
 * leaves the JVM heap.
 *
 * ## Failure semantics
 *
 * If reading a file or a [BlobSink.processBatch] call throws, the exception propagates: the
 * per-file [coroutineScope] cancels that file's other batches and rethrows, which cancels every
 * sibling file. The **failed** blob's sink is **not committed**, so it publishes no partial output;
 * its [BlobSink.close] still runs (under [NonCancellable]) to release resources, so a cancelled
 * blob never leaks.
 *
 * This is a **per-blob, not per-upload** guarantee: blobs that committed *before* the failure have
 * already published their output. There is no upload-level atomicity — so when the WorkItem is
 * retried per the pipeline's failure policy, those already-published blobs are produced again. The
 * retry (or the sink's output naming) MUST therefore be idempotent per blob to avoid duplicates.
 *
 * @property rawImpressionUploadFilesStub client used to list the upload's files.
 * @property rawImpressionUpload resource name of the upload to read, format
 *   `dataProviders/{data_provider}/rawImpressionUploads/{raw_impression_upload}`.
 * @property eventIdColumn parquet column holding the event id (used for the digest). A `STRING`
 *   (BINARY+STRING) or raw `BINARY` column; both are accepted. Downstream reads any other columns
 *   it needs (e.g. event time for per-model-line windowing) from [DigestedEvent.row].
 */
class RawImpressionSource(
  private val parquetStorageClient: ParquetStorageClient,
  private val rawImpressionUploadFilesStub: RawImpressionUploadFileServiceCoroutineStub,
  private val rawImpressionUpload: String,
  private val eventIdColumn: String,
  private val shardIndex: Int = 0,
  private val totalShards: Int = 1,
  private val eventIdDigestExtractor: EventIdDigestExtractor,
  // OpenTelemetry instruments (read/drop/emit counters + per-file latency
  // histogram) that surface on operator dashboards via EdpaTelemetry.
  private val metrics: RawImpressionSourceMetrics = RawImpressionSourceMetrics(),
  // Elastic dispatcher for the per-file coroutines. MUST tolerate blocking work
  // (the parquet read is synchronous), so the default is Dispatchers.IO.
  private val readDispatcher: CoroutineContext = Dispatchers.IO,
  // CPU dispatcher for batch processing; capped to [workers] via limitedParallelism.
  private val workerDispatcher: CoroutineDispatcher = Dispatchers.Default,
  // Max input files open at once: the memory/I/O governor for reading. Defaults to
  // ~2× cores — enough open readers to keep the CPU pool fed while some block on GCS.
  private val maxOpenFiles: Int = DEFAULT_MAX_OPEN_FILES,
  // CPU parallelism for batch processing (the bottleneck). Defaults to ~#cores.
  private val workers: Int = DEFAULT_WORKERS,
  // Number of shard-surviving events per batch (amortizes per-batch handoff).
  private val batchSize: Int = DEFAULT_BATCH_SIZE,
  // Read-ahead / memory bound: max batches launched-but-unfinished at once. Keep
  // it > [workers] so finishing workers always have a decoded batch queued.
  private val maxInFlightBatches: Int = DEFAULT_MAX_IN_FLIGHT_BATCHES,
  // File-list mode (Phase 2): when non-null, read exactly these `RawImpressionUploadFile`
  // resource names (each resolved to its `blob_uri` by name) read whole, with NO
  // fingerprint-shard filter and NO whole-upload discovery. When null (Phase 0), discover all
  // of [rawImpressionUpload]'s files and apply the shard filter.
  private val inputFiles: List<String>? = null,
) {
  init {
    require(totalShards > 0) { "totalShards must be positive, got $totalShards" }
    require(shardIndex in 0 until totalShards) {
      "shardIndex ($shardIndex) must be in [0, totalShards=$totalShards)"
    }
    require(maxOpenFiles > 0) { "maxOpenFiles must be positive, got $maxOpenFiles" }
    require(workers > 0) { "workers must be positive, got $workers" }
    require(batchSize > 0) { "batchSize must be positive, got $batchSize" }
    require(maxInFlightBatches > 0) {
      "maxInFlightBatches must be positive, got $maxInFlightBatches"
    }
    require(rawImpressionUpload.isNotBlank()) { "rawImpressionUpload must be non-blank" }
    require(eventIdColumn.isNotBlank()) { "eventIdColumn must be non-blank" }
    if (inputFiles != null) {
      // File-list mode reads exactly the given files (an empty list reads nothing, matching the
      // prior whole-upload behavior for an upload with no files); it never applies the shard
      // filter.
      require(shardIndex == 0 && totalShards == 1) {
        "file-list mode does not shard; leave shardIndex/totalShards at their defaults"
      }
    }
  }

  /**
   * Caller-supplied, per-blob consumer of shard-filtered event batches. One is minted per file by
   * the `openSink` lambda passed to [streamBlobs]. This class is agnostic to what [processBatch]
   * does (label + write per-blob output, or accumulate into a shared map, …).
   *
   * Threading: [processBatch] is invoked **concurrently by the CPU pool** for the same blob, so it
   * MUST be thread-safe or serialize internally (e.g. forward to a single per-blob writer coroutine
   * over a channel). [commit] runs once on success (after every [processBatch] has returned);
   * [close] always runs once to release resources (so a cancelled blob never leaks).
   * - **Phase 2 (labeling):** per-blob output. `processBatch` labels each event and streams the
   *   records to this blob's own output (typically via an internal channel + one writer coroutine
   *   so the output is single-threaded); `commit` finalizes/uploads it and `close` releases the
   *   writer.
   * - **Phase 0 (pool assignment):** `processBatch` records each event's `(subpoolId,
   *   eventIdDigest)` into a single shared map (concurrency-safe — e.g. a striped wrapper around
   *   `Bytes12IntMap`); `commit`/`close` are no-ops and the whole map is uploaded once after
   *   [streamBlobs] returns.
   */
  interface BlobSink {
    /** Processes one shard-filtered batch for this blob. Called concurrently. */
    suspend fun processBatch(events: List<ParquetDigestedEvent>)

    /**
     * Finalizes + publishes this blob's output (Phase 2 upload; Phase 0 no-op). Called exactly
     * once, **only on success**, after every [processBatch] has returned.
     */
    suspend fun commit()

    /**
     * Releases resources (e.g. an open output stream). **Always** called exactly once — after
     * [commit] on success, or alone on failure/cancellation. MUST be idempotent and MUST NOT
     * publish partial output. Should not throw; if it does during a failure it is attached as a
     * suppressed exception (it never masks the original processing failure), and on the success
     * path it surfaces normally.
     */
    suspend fun close()
  }

  /**
   * Streams this VM's shard of the upload exactly once and suspends until every file has been fully
   * read and processed. Decode + decrypt + event-id digest + the shard filter are paid once per row
   * in the reader.
   *
   * Delivers **every** row that passes the shard filter — including **duplicate [EventIdDigest]s**:
   * the same event id can appear in many impressions across files, and no de-duplication is
   * performed here. Callers that need unique digests (e.g. Phase 0's subpool map keyed by
   * [EventIdDigest]) are responsible for de-duplicating in their [BlobSink].
   *
   * @param openSink mints a fresh [BlobSink] for one input file, given its `blobUri`. Called once
   *   per file. The returned sink's [BlobSink.processBatch] is invoked concurrently (see
   *   [BlobSink]); any state shared across files captured by this lambda must be concurrency-safe.
   */
  suspend fun streamBlobs(openSink: suspend (blobUri: String) -> BlobSink) {
    val blobUris = discoverBlobUris()
    logger.info(
      "Starting raw-impression read of ${blobUris.size} file(s) for shard " +
        "$shardIndex/$totalShards of upload $rawImpressionUpload"
    )
    val progress = ProgressTracker(blobUris.size)
    val openFiles = Semaphore(maxOpenFiles)
    // Per-call (not instance fields): the shared CPU cap and the read-ahead bound.
    // Scoping them here keeps each streamBlobs call self-contained — a failed run
    // can abandon in-flight permits without throttling a later call on the same
    // instance.
    val cpuDispatcher: CoroutineDispatcher = workerDispatcher.limitedParallelism(workers)
    val inFlight = Semaphore(maxInFlightBatches)
    coroutineScope {
      for (blobUri in blobUris) {
        launch(readDispatcher) {
          openFiles.withPermit { processBlob(blobUri, openSink, cpuDispatcher, inFlight, progress) }
        }
      }
    }
    progress.logSummary()
  }

  /**
   * Reads one input file on its own coroutine: mints its [BlobSink], launches each batch onto the
   * shared `cpuDispatcher` inside a per-file [coroutineScope] (which joins them, giving per-file
   * completion), then commits and always closes.
   */
  private suspend fun processBlob(
    blobUri: String,
    openSink: suspend (blobUri: String) -> BlobSink,
    cpuDispatcher: CoroutineDispatcher,
    inFlight: Semaphore,
    progress: ProgressTracker,
  ) {
    val startTime: TimeSource.Monotonic.ValueTimeMark = TimeSource.Monotonic.markNow()
    logger.fine { "Reading raw-impression file $blobUri" }
    val sink = openSink(blobUri)
    var primary: Throwable? = null
    try {
      // The per-file scope owns this file's batch jobs; it returns the reader's
      // tallies after reading finishes AND after every launched batch has joined.
      val counts = coroutineScope {
        readEventsFromBlob(blobUri) { batch ->
          // Backpressure: suspend the reader once maxInFlightBatches are in
          // flight. The permit is held until this batch finishes (release in the
          // finally — load-bearing; missing it would deadlock the reader).
          inFlight.acquire()
          launch(cpuDispatcher) {
            try {
              sink.processBatch(batch)
            } finally {
              inFlight.release()
            }
          }
        }
      }
      // Reached only on success: a thrown batch/read cancels the scope first.
      sink.commit()
      metrics.fileProcessingDurationHistogram.record(
        startTime.elapsedNow().inWholeMilliseconds / 1000.0
      )
      logger.fine {
        "Read ${counts.read} row(s) from $blobUri " +
          "(emitted ${counts.emitted}, dropped ${counts.droppedOtherShard})"
      }
      progress.recordFileComplete(counts)
    } catch (t: Throwable) {
      primary = t
      throw t
    } finally {
      // Always release the sink's resources, even on cancellation (a sibling
      // failure), so an open-but-uncommitted sink never leaks. NonCancellable lets
      // the suspend close() run after this coroutine is already cancelled; close()
      // must not publish partial output. If close() itself throws, don't let it
      // mask an in-flight processing failure — attach it as suppressed; on the
      // success path (no primary), let it surface.
      try {
        withContext(NonCancellable) { sink.close() }
      } catch (closeError: Throwable) {
        val pending = primary
        if (pending != null) pending.addSuppressed(closeError) else throw closeError
      }
    }
  }

  internal fun belongsToShard(digest: EventIdDigest): Boolean {
    // SHA-256's avalanche makes the upper Long a uniform shard hash on its own.
    val shard = Math.floorMod(digest.high, totalShards.toLong()).toInt()
    return shard == shardIndex
  }

  /**
   * Reads one blob, computes the event-id digest, applies the shard filter, and invokes [onBatch]
   * for each full batch of survivors (a fresh list each time, so it is safe to hand to a concurrent
   * worker). Runs on the file's reader coroutine; returns the file's row tallies.
   */
  private suspend fun readEventsFromBlob(
    blobUri: String,
    onBatch: suspend (List<ParquetDigestedEvent>) -> Unit,
  ): FileCounts {
    val parquetBlob =
      parquetStorageClient.getBlob(blobUri) ?: error("Raw-impression blob not found: $blobUri")
    var read = 0L
    var droppedOtherShard = 0L
    var emitted = 0L
    var batch = ArrayList<ParquetDigestedEvent>(batchSize)
    parquetBlob.readRows().collect { row ->
      read++
      val digest = eventIdDigestExtractor.extract(readEventIdBytes(row, blobUri))
      if (!belongsToShard(digest)) {
        droppedOtherShard++
        return@collect
      }
      emitted++
      batch.add(DigestedEvent(row, digest))
      if (batch.size >= batchSize) {
        onBatch(batch)
        batch = ArrayList(batchSize)
      }
    }
    if (batch.isNotEmpty()) onBatch(batch)
    // Aggregate per file, then emit to the OTel counters once (instead of per row).
    metrics.rowsReadCounter.add(read)
    metrics.rowsDroppedOtherShardCounter.add(droppedOtherShard)
    metrics.rowsEmittedCounter.add(emitted)
    return FileCounts(read, droppedOtherShard, emitted)
  }

  /**
   * Returns the Cloud Storage `blob_uri`s to read. In file-list mode ([inputFiles] non-null),
   * resolves exactly those `RawImpressionUploadFile` resource names to their `blob_uri`s by name
   * (no whole-upload listing). Otherwise lists all of the [rawImpressionUpload]'s files via the
   * metadata service (paginated) and returns their `blob_uri`s.
   */
  private suspend fun discoverBlobUris(): List<String> {
    if (inputFiles != null) {
      return inputFiles.map { fileName ->
        try {
          rawImpressionUploadFilesStub
            .getRawImpressionUploadFile(getRawImpressionUploadFileRequest { name = fileName })
            .blobUri
        } catch (e: StatusException) {
          throw Exception("Error getting RawImpressionUploadFile $fileName", e)
        }
      }
    }
    val blobUris = mutableListOf<String>()
    rawImpressionUploadFilesStub
      .listResources { pageToken: String ->
        val response =
          try {
            listRawImpressionUploadFiles(
              listRawImpressionUploadFilesRequest {
                parent = rawImpressionUpload
                pageSize = LIST_PAGE_SIZE
                this.pageToken = pageToken
              }
            )
          } catch (e: StatusException) {
            throw Exception("Error listing RawImpressionUploadFiles for $rawImpressionUpload", e)
          }
        ResourceList(response.rawImpressionUploadFilesList, response.nextPageToken)
      }
      .collect { page -> page.forEach { blobUris.add(it.blobUri) } }
    return blobUris
  }

  /**
   * Reads the [eventIdColumn] from [row] as bytes. Parquet may surface it either as a
   * `STRING_VALUE` (BINARY+STRING) or a `BYTES_VALUE` (raw BINARY); both are accepted.
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
   * Concurrency-safe progress logging across files. Files complete on many coroutines, so
   * [recordFileComplete] is `@Synchronized`. Logs a line every ~10% of files completed;
   * [logSummary] logs the final totals. Keeps its own in-process counts (for the log) independent
   * of the OTel counters in [metrics] (which feed dashboards).
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

    /** ~2× cores: enough open readers to keep the CPU pool fed despite GCS stalls. */
    private val DEFAULT_MAX_OPEN_FILES: Int =
      maxOf(1, Runtime.getRuntime().availableProcessors() * 2)

    /** Shard-surviving events per batch. */
    private const val DEFAULT_BATCH_SIZE = 256

    /** Read-ahead depth in batches (> DEFAULT_WORKERS so the CPU pool stays fed). */
    private const val DEFAULT_MAX_IN_FLIGHT_BATCHES = 64

    /** Page size for listing the upload's files. */
    private const val LIST_PAGE_SIZE = 1000
  }
}
