/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.dataavailability

import io.opentelemetry.api.common.Attributes
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking

/**
 * A delete event to be buffered for batch processing.
 *
 * @property deletedBlobPath The GCS path of the deleted blob.
 * @property impressionMetadataResourceId Optional resource ID for direct deletion.
 */
data class DeleteEvent(
  val deletedBlobPath: String,
  val impressionMetadataResourceId: String?,
)

/**
 * Buffers delete events in memory and flushes them in batches.
 *
 * On Cloud Functions Gen2, a single instance can handle concurrent requests. This class exploits
 * instance reuse to accumulate delete events across invocations and process them together, reducing
 * per-event overhead (gRPC channel setup, Spanner round-trips) when lifecycle rules delete many
 * files in a short window.
 *
 * Events are flushed when:
 * - The buffer reaches [batchSize] events, OR
 * - A periodic timer fires every [flushIntervalSeconds] seconds.
 *
 * Failed events are re-queued for the next flush cycle. A shutdown hook should call [shutdown] to
 * drain remaining events before the instance is terminated.
 *
 * @property cleanupDelegate The underlying cleanup logic that processes individual events.
 * @property batchSize Flush immediately when this many events are buffered.
 * @property flushIntervalSeconds Periodic flush interval in seconds.
 * @property metrics Metrics recorder for telemetry.
 */
class BufferedDataAvailabilityCleanup(
  private val cleanupDelegate: DataAvailabilityCleanup,
  private val batchSize: Int = DEFAULT_BATCH_SIZE,
  private val flushIntervalSeconds: Long = DEFAULT_FLUSH_INTERVAL_SECONDS,
  private val metrics: DataAvailabilityCleanupMetrics = DataAvailabilityCleanupMetrics(),
) {
  private val queue = ConcurrentLinkedQueue<DeleteEvent>()
  private val scheduler: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor { r ->
      Thread(r, "cleanup-buffer-flush").apply { isDaemon = true }
    }
  private val flushScheduled = AtomicBoolean(false)

  /**
   * Adds a delete event to the buffer.
   *
   * If the buffer reaches [batchSize], triggers an immediate flush.
   */
  fun enqueue(event: DeleteEvent) {
    queue.add(event)
    logger.info("Buffered delete event for: ${event.deletedBlobPath} (queue size: ${queue.size})")

    ensureFlushScheduled()

    if (queue.size >= batchSize) {
      logger.info("Batch size threshold ($batchSize) reached, flushing immediately")
      flush()
    }
  }

  private fun ensureFlushScheduled() {
    if (flushScheduled.compareAndSet(false, true)) {
      scheduler.scheduleAtFixedRate(
        { flush() },
        flushIntervalSeconds,
        flushIntervalSeconds,
        TimeUnit.SECONDS,
      )
      logger.info("Scheduled periodic flush every ${flushIntervalSeconds}s")
    }
  }

  /** Drains all buffered events and processes them sequentially. Failed events are re-queued. */
  fun flush() {
    val batch = drain()
    if (batch.isEmpty()) return

    logger.info("Flushing ${batch.size} buffered delete events")
    var successCount = 0
    var failedCount = 0

    for (event in batch) {
      try {
        runBlocking {
          cleanupDelegate.cleanup(event.deletedBlobPath, event.impressionMetadataResourceId)
        }
        successCount++
      } catch (e: Exception) {
        failedCount++
        logger.log(
          Level.WARNING,
          "Failed to clean up ${event.deletedBlobPath}, re-queuing",
          e,
        )
        queue.add(event)
      }
    }

    logger.info(
      "Flush complete: $successCount succeeded, $failedCount failed (re-queued), " +
        "${queue.size} remaining in buffer"
    )

    if (successCount > 0) {
      metrics.recordsDeletedCounter.add(
        successCount.toLong(),
        Attributes.of(
          DataAvailabilityCleanupMetrics.CLEANUP_STATUS_ATTR,
          BATCH_FLUSH_STATUS,
        ),
      )
    }
  }

  private fun drain(): List<DeleteEvent> {
    val batch = mutableListOf<DeleteEvent>()
    while (true) {
      queue.poll()?.let { batch.add(it) } ?: break
    }
    return batch
  }

  /** Flushes remaining events and shuts down the periodic scheduler. */
  fun shutdown() {
    logger.info("Shutting down buffered cleanup, flushing remaining events")
    flush()
    scheduler.shutdown()
    try {
      if (!scheduler.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        scheduler.shutdownNow()
      }
    } catch (e: InterruptedException) {
      scheduler.shutdownNow()
      Thread.currentThread().interrupt()
    }
  }

  /** Returns the number of events currently buffered. */
  fun pendingCount(): Int = queue.size

  companion object {
    private val logger: Logger =
      Logger.getLogger(BufferedDataAvailabilityCleanup::class.java.name)
    const val DEFAULT_BATCH_SIZE = 100
    const val DEFAULT_FLUSH_INTERVAL_SECONDS = 30L
    private const val SHUTDOWN_TIMEOUT_SECONDS = 10L
    private const val BATCH_FLUSH_STATUS = "batch_flush"
  }
}
