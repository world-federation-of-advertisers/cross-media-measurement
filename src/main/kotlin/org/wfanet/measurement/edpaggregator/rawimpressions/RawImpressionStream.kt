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
import java.time.Instant
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.wfanet.measurement.edpaggregator.vidlabeler.utils.ActiveWindow
import org.wfanet.measurement.edpaggregator.vidlabeler.utils.instantToEpochMicros
import org.wfanet.measurement.storage.ParquetStorageClient

/**
 * Streams the local VM's shard of raw impressions for one upload and runs the
 * full read → decrypt → fingerprint → shard-filter → caller-supplied work chain
 * **inline, one coroutine per file, in parallel across files** — labeling each
 * surviving event against one or more model lines in a single pass.
 *
 * ## Execution model (why it is shaped this way)
 *
 * The read path is CPU-heavy (parquet decode + PME AES-GCM decrypt +
 * decompression) on top of blocking GCS I/O, and there are always far more
 * files than cores. So the most efficient model is:
 *
 *  - **One coroutine per file**, launched on [readDispatcher] (an *elastic*
 *    dispatcher — [Dispatchers.IO] by default — so a coroutine blocked on a GCS
 *    read does not pin a core; siblings keep the CPU busy).
 *  - **A single [Semaphore] of [maxConcurrentReaders]** is the one governor for
 *    both peak memory (open readers × prefetch buffers) and effective CPU
 *    concurrency. It defaults to ~1.5× cores: enough slack that while some files
 *    block on the network others keep the cores doing decrypt/fingerprint/label,
 *    without so many simultaneous CPU-bound coroutines that the cores thrash.
 *  - **The whole chain runs inline on the file's coroutine.** The decoded row
 *    (`Map<String, Any?>`) is fingerprinted and handed to the caller's work on
 *    the same core that decoded it — cache-hot — and is then discarded.
 *
 * With ≫ files than cores, this file-level parallelism alone saturates the
 * cores; there is no serial collector to bottleneck on, and no intra-file
 * parallelism is needed (the parquet reader is sequential per file anyway).
 *
 * ## Per-file consumers & concurrency
 *
 * For each input file, [stream] calls [ModelLineConsumerFactory.open] to create
 * a fresh [ModelLineConsumer] per model line, **confined to that file's
 * coroutine**: `open(blobKey)` → `process(event)` for each in-window event →
 * `close()` once the file is fully read. Because a consumer is created and used
 * on a single coroutine, its own state (e.g. a per-file output writer) is
 * single-threaded and needs **no synchronization**. Only state shared *across*
 * files (e.g. Phase 0's per-subpool accumulators captured in the factory) must
 * be concurrency-safe — shard it by owner or synchronize it.
 *
 * ## Filtering split
 *
 *  - **Shard filter** (`fp.high % totalShards == shardIndex`) is applied here:
 *    it is fingerprint-only, hence identical for every model line in a WorkItem,
 *    so it is paid once in the shared pass.
 *  - **Active-window filter** is per-model-line and is applied by [stream] via
 *    each consumer's [ActiveWindow][ModelLineConsumer.window]; the stream itself
 *    is window-agnostic.
 *
 * ## Blob discovery
 *
 * [doneBlobKey] is the blob key (relative to the [parquetStorageClient] root) of
 * the `done` marker an EDP writes at the end of an upload. The containing folder
 * is listed and every blob except the marker is treated as a raw-impression
 * parquet file.
 *
 * ## Encryption
 *
 * PME decryption is handled declaratively by the injected [parquetStorageClient]
 * (configure it with a `ParquetDecryptionConfig` pointing at the EDP's footer
 * keys). Plaintext column data never leaves the JVM heap.
 *
 * ## Column mapping
 *
 * [labelerInputFieldMapping] mirrors
 * `VidLabelingConfig.ModelLineConfig.labeler_input_field_mapping` (key =
 * LabelerInput field path, value = parquet column name). Required entries:
 *  - `timestamp_usec` -> parquet INT64 column (epoch micros), or an
 *    INT64+TIMESTAMP column (decoded as `Instant`); both are accepted.
 *  - `event_id.id` -> parquet BINARY/STRING column with the event id bytes.
 */
class RawImpressionStream(
  private val parquetStorageClient: ParquetStorageClient,
  private val doneBlobKey: String,
  private val labelerInputFieldMapping: Map<String, String>,
  private val shardIndex: Int,
  private val totalShards: Int,
  private val fingerprintExtractor: FingerprintExtractor,
  // Elastic dispatcher for the per-file coroutines. MUST tolerate blocking work
  // (the parquet read is synchronous), so the default is Dispatchers.IO. The
  // CPU concurrency is governed by maxConcurrentReaders, not by this dispatcher.
  private val readDispatcher: CoroutineContext = Dispatchers.IO,
  // The single concurrency governor: how many files are read+processed at once.
  // Bounds both peak memory and effective CPU concurrency. Defaults to ~1.5×
  // cores to hide GCS stalls without oversubscribing the cores.
  private val maxConcurrentReaders: Int = DEFAULT_MAX_CONCURRENT_READERS,
) {
  /**
   * Per-(input file, model line) consumer. Created fresh for each file by a
   * [ModelLineConsumerFactory], so it is confined to one file's coroutine and
   * its own state (e.g. an output writer) needs no synchronization.
   *
   * Lifecycle, all on the file's coroutine: `open(blobKey)` (factory) →
   * [process] for each in-window, shard-surviving event → [close] once the file
   * is fully read.
   */
  interface ModelLineConsumer {
    /** This model line's active window; events whose time is outside it are skipped. */
    val window: ActiveWindow

    /**
     * Handles one in-window event for the current file. Typically projects the
     * row into a `LabelerInput`, runs the labeler for this model line, and
     * appends/routes the result.
     */
    suspend fun process(event: FingerprintedEvent)

    /**
     * The current file has been fully read. Flush + upload this model line's
     * output for this file (Phase 2), or no-op (Phase 0, which accumulates into
     * shared per-subpool state and uploads once after [stream] returns).
     *
     * Called only on **successful** completion of the file: if the file read
     * fails, the exception propagates and [close] is NOT called, so a partial
     * output is never uploaded (see [stream]'s failure semantics).
     */
    suspend fun close()
  }

  /**
   * Opens one [ModelLineConsumer] per input file, for a single model line.
   * [open] receives the source `blobKey`, so per-file outputs can be keyed/named
   * by it. The returned consumer is used by exactly one coroutine.
   */
  fun interface ModelLineConsumerFactory {
    suspend fun open(blobKey: String): ModelLineConsumer
  }

  /** Atomic counters updated as the chain runs (safe under parallel files). */
  data class Stats(
    val read: AtomicLong = AtomicLong(),
    val droppedOtherShard: AtomicLong = AtomicLong(),
    val emitted: AtomicLong = AtomicLong(),
  )

  val stats: Stats = Stats()

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

  init {
    require(totalShards > 0) { "totalShards must be positive, got $totalShards" }
    require(shardIndex in 0 until totalShards) {
      "shardIndex ($shardIndex) must be in [0, totalShards=$totalShards)"
    }
    require(maxConcurrentReaders > 0) {
      "maxConcurrentReaders must be positive, got $maxConcurrentReaders"
    }
    require(doneBlobKey.isNotBlank()) { "doneBlobKey must be non-blank" }
  }

  /**
   * Streams this VM's shard of the upload exactly once and runs the per-file
   * lifecycle of each model line's consumer, inline and in parallel across files
   * (≤ [maxConcurrentReaders] concurrent). For every input file, on its own
   * coroutine: open one consumer per factory, deliver each shard-surviving event
   * to every consumer whose window contains it, then close every consumer.
   * Decode + decrypt + fingerprint are paid once per row; suspends until every
   * file has been fully processed.
   *
   * ## How many consumers (Phase 0 vs Phase 2)
   *
   *  - **Phase 0 (pool assignment):** at most ONE model line per WorkItem, so
   *    `consumerFactories` has size 1. The consumer labels in pool-emit mode and
   *    routes `(subpoolId, fingerprint)` into shared per-subpool accumulators;
   *    `close()` is a no-op and the accumulators are uploaded once after this
   *    call returns.
   *  - **Phase 2 (labeling):** multiple model lines appear in one WorkItem ONLY
   *    when they do NOT require memoization — i.e. no per-model-line rank-index
   *    map has to be pulled from GCS — so labeling several in parallel adds no
   *    large in-memory state and is safe. When a model line DOES require
   *    memoization (its rank-index blob, a large file, must be loaded), Phase 1
   *    emits a WorkItem with a SINGLE model line, so at most one big rank-index
   *    map is resident at a time and this design cannot OOM on that account.
   *
   * ## Output-side memory caveat
   *
   * In Phase 2 each consumer buffers one output (mesos record-IO) file in memory
   * per (input file, model line) until `close()` uploads it, so N model lines ⇒
   * up to N output files resident per in-flight input file. Each output is much
   * smaller than its input because this VM keeps only events surviving the
   * fingerprint shard filter (~1/totalShards).
   * TODO(@marcopremier): benchmark peak memory of this output-buffering approach
   * (maxConcurrentReaders × N model lines × per-file output buffer) and bound it
   * if it proves material.
   *
   * ## Failure semantics
   *
   * If reading/processing a file throws, the exception propagates, the enclosing
   * [coroutineScope] cancels sibling files, and that file's consumers are NOT
   * closed — so no partial output is uploaded (per-file upload stays atomic).
   * The WorkItem is then retried per the pipeline's failure policy.
   *
   * @param consumerFactories one factory per model line; each `open` MUST return
   *   a fresh consumer (its per-file state is thread-confined). State shared
   *   across files (captured in the factory) must be concurrency-safe.
   */
  suspend fun stream(consumerFactories: List<ModelLineConsumerFactory>) {
    require(consumerFactories.isNotEmpty()) { "consumerFactories must be non-empty" }
    val semaphore = Semaphore(maxConcurrentReaders)
    coroutineScope {
      for (blobKey in discoverBlobKeys()) {
        launch(readDispatcher) {
          semaphore.withPermit {
            // Fresh consumer per (file, model line): thread-confined to this coroutine.
            val consumers = consumerFactories.map { it.open(blobKey) }
            processFile(blobKey) { event ->
              val eventMicros = readEventMicros(event.row)
              for (consumer in consumers) {
                if (consumer.window.contains(eventMicros)) consumer.process(event)
              }
            }
            // File fully read → flush + upload each model line's output for this file.
            // Reached only on success; a failure above propagates and skips this.
            for (consumer in consumers) consumer.close()
          }
        }
      }
    }
  }

  internal fun belongsToShard(fingerprint: Fingerprint): Boolean {
    // SHA-256's avalanche makes the upper Long a uniform shard hash on its own.
    val shard = Math.floorMod(fingerprint.high, totalShards.toLong()).toInt()
    return shard == shardIndex
  }

  /**
   * Reads one blob, fingerprints + shard-filters each row, and invokes [onEvent]
   * inline for every surviving event. Runs entirely on the calling file
   * coroutine.
   */
  private suspend fun processFile(
    blobKey: String,
    onEvent: suspend (FingerprintedEvent) -> Unit,
  ) {
    val parquetBlob =
      parquetStorageClient.getBlob(blobKey) ?: error("Raw-impression blob not found: $blobKey")
    parquetBlob.readRows().collect { row ->
      stats.read.incrementAndGet()
      val fingerprint = fingerprintExtractor.extract(readEventIdBytes(row, blobKey))
      if (!belongsToShard(fingerprint)) {
        stats.droppedOtherShard.incrementAndGet()
        return@collect
      }
      stats.emitted.incrementAndGet()
      onEvent(FingerprintedEvent(row, fingerprint))
    }
  }

  /**
   * Lists the folder containing [doneBlobKey] and drops the done marker,
   * returning the blob keys of the remaining raw-impression files.
   */
  private suspend fun discoverBlobKeys(): List<String> {
    val folderPrefix: String = doneBlobKey.substringBeforeLast("/")
    return parquetStorageClient
      .listBlobs(folderPrefix)
      .map { it.blobKey }
      .toList()
      .filterNot { isDoneMarker(it) }
  }

  private fun isDoneMarker(blobKey: String): Boolean =
    blobKey.substringAfterLast("/").equals(DONE_MARKER_FILE_NAME, ignoreCase = true)

  /**
   * Reads the event-id column from [row] as bytes. Parquet may surface the
   * `event_id.id` column either as a [String] (BINARY+STRING) or a [ByteString]
   * (raw BINARY); both are accepted.
   */
  private fun readEventIdBytes(row: Map<String, Any?>, blobKey: String): ByteString =
    when (val v = row[eventIdColumn]) {
      is String -> ByteString.copyFromUtf8(v)
      is ByteString -> v
      else ->
        error(
          "event-id column '$eventIdColumn' of '$blobKey' has unexpected type " +
            "${v?.javaClass?.simpleName} (expected String or ByteString)"
        )
    }

  /**
   * Reads the event-time column from [row] as epoch microseconds. Accepts a
   * [Long] (plain INT64 epoch micros) or an [Instant] (INT64+TIMESTAMP), and
   * normalizes to a primitive `long` — neither path allocates on the hot path.
   */
  private fun readEventMicros(row: Map<String, Any?>): Long =
    when (val v = row.getValue(eventTimeColumn)) {
      is Long -> v
      is Instant -> instantToEpochMicros(v)
      else ->
        error(
          "event-time column '$eventTimeColumn' has unexpected type " +
            "${v?.javaClass?.simpleName} (expected Long epoch micros or Instant)"
        )
    }

  companion object {
    /** ~1.5× cores: hides GCS stalls without oversubscribing the cores. */
    private val DEFAULT_MAX_CONCURRENT_READERS: Int =
      maxOf(1, (Runtime.getRuntime().availableProcessors() * 3 + 1) / 2)

    /** Marker file name written at the end of an upload. */
    private const val DONE_MARKER_FILE_NAME = "done"

    /** Well-known LabelerInput field path for event timestamp (epoch micros). */
    private const val EVENT_TIME_FIELD_PATH = "timestamp_usec"

    /** Well-known LabelerInput field path for the publisher-specific event id. */
    private const val EVENT_ID_FIELD_PATH = "event_id.id"
  }
}
