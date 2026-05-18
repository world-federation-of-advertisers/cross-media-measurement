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
import kotlin.time.TimeSource
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.batchDeleteImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataRequest
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.StorageClient

data class DeleteEvent(
  val deletedBlobPath: String,
  val impressionMetadataResourceId: String?,
)

/**
 * Buffers delete events in memory and flushes them via `BatchDeleteImpressionMetadata` RPCs,
 * each capped at [MAX_BATCH_DELETE_SIZE] records.
 *
 * On Cloud Functions Gen2, a single instance can handle concurrent requests. This class exploits
 * instance reuse to accumulate delete events across invocations and process them together,
 * rather than N individual RPCs.
 *
 * Events are flushed when:
 * - The buffer reaches [batchSize] events, OR
 * - A periodic timer fires every [flushIntervalSeconds] seconds.
 *
 * During flush, events without a resource ID are resolved by looking up the blob URI in the
 * ImpressionMetadata service. Events whose blobs still have a live version in storage are skipped
 * (noncurrent version deletion). Resolved resource names are chunked into groups of
 * [MAX_BATCH_DELETE_SIZE] and each chunk is deleted via a single batch RPC. Events that fail
 * resolution are re-queued.
 */
class BufferedDataAvailabilityCleanup(
  private val impressionMetadataServiceStub: ImpressionMetadataServiceCoroutineStub,
  private val dataProviderName: String,
  private val storageClient: StorageClient,
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

  fun enqueue(event: DeleteEvent) {
    queue.add(event)
    val size = queue.size
    if (size % LOG_INTERVAL == 0 || size <= 10) {
      logger.info(
        "Buffered delete event for: ${event.deletedBlobPath} (queue size: $size)"
      )
    }

    ensureFlushScheduled()

    if (size >= batchSize) {
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

  fun flush() {
    val batch = drain()
    if (batch.isEmpty()) return

    val flushStart = TimeSource.Monotonic.markNow()
    logger.info("Flushing ${batch.size} buffered delete events")

    runBlocking {
      val resourceNames = mutableListOf<String>()
      var skippedCount = 0
      var resolveFailCount = 0

      val resolveStart = TimeSource.Monotonic.markNow()
      for (event in batch) {
        try {
          val blobUri = SelectedStorageClient.parseBlobUri(event.deletedBlobPath)
          val liveBlob = storageClient.getBlob(blobUri.key)
          if (liveBlob != null) {
            skippedCount++
            continue
          }

          val resourceName = resolveResourceName(event)
          if (resourceName != null) {
            resourceNames.add(resourceName)
          } else {
            skippedCount++
          }
        } catch (e: Exception) {
          logger.log(
            Level.WARNING,
            "Failed to resolve ${event.deletedBlobPath}, re-queuing",
            e,
          )
          resolveFailCount++
          queue.add(event)
        }
      }
      val resolveMs = resolveStart.elapsedNow().inWholeMilliseconds
      logger.info(
        "Resolved ${resourceNames.size} names, skipped $skippedCount, " +
          "re-queued $resolveFailCount in ${resolveMs}ms"
      )

      if (resourceNames.isNotEmpty()) {
        val chunks = resourceNames.chunked(MAX_BATCH_DELETE_SIZE)
        logger.info(
          "Deleting ${resourceNames.size} records in ${chunks.size} batch RPC(s) " +
            "(max $MAX_BATCH_DELETE_SIZE per RPC)"
        )
        val deleteStart = TimeSource.Monotonic.markNow()
        var totalDeleted = 0

        for ((index, chunk) in chunks.withIndex()) {
          try {
            impressionMetadataServiceStub.batchDeleteImpressionMetadata(
              batchDeleteImpressionMetadataRequest {
                parent = dataProviderName
                names += chunk
              }
            )
            totalDeleted += chunk.size
            logger.info(
              "Batch RPC ${index + 1}/${chunks.size}: deleted ${chunk.size} records"
            )
          } catch (e: Exception) {
            logger.log(
              Level.SEVERE,
              "Batch RPC ${index + 1}/${chunks.size} failed for ${chunk.size} records, re-queuing",
              e,
            )
            chunk.forEach { name -> queue.add(DeleteEvent(name, name)) }
          }
        }

        val deleteMs = deleteStart.elapsedNow().inWholeMilliseconds
        val totalMs = flushStart.elapsedNow().inWholeMilliseconds
        logger.info(
          "Flush complete: $totalDeleted deleted, $skippedCount skipped, " +
            "$resolveFailCount re-queued, ${queue.size} remaining " +
            "[resolve=${resolveMs}ms, delete=${deleteMs}ms, total=${totalMs}ms]"
        )
        metrics.recordsDeletedCounter.add(
          totalDeleted.toLong(),
          Attributes.of(
            DataAvailabilityCleanupMetrics.CLEANUP_STATUS_ATTR,
            BATCH_FLUSH_STATUS,
          ),
        )
      } else {
        val totalMs = flushStart.elapsedNow().inWholeMilliseconds
        logger.info(
          "No events to batch-delete ($skippedCount skipped) [total=${totalMs}ms]"
        )
      }
    }
  }

  private suspend fun resolveResourceName(event: DeleteEvent): String? {
    if (!event.impressionMetadataResourceId.isNullOrEmpty()) {
      return event.impressionMetadataResourceId
    }

    val listResponse =
      impressionMetadataServiceStub.listImpressionMetadata(
        listImpressionMetadataRequest {
          parent = dataProviderName
          filter = ListImpressionMetadataRequestKt.filter {
            blobUriPrefix = event.deletedBlobPath
          }
        }
      )

    val results = listResponse.impressionMetadataList
    return when {
      results.isEmpty() -> {
        logger.warning(
          "No ImpressionMetadata found for blob URI: ${event.deletedBlobPath}. Skipping."
        )
        null
      }
      results.size > 1 -> {
        logger.warning(
          "Multiple ImpressionMetadata records (${results.size}) found for " +
            "blob URI: ${event.deletedBlobPath}. Skipping."
        )
        null
      }
      else -> results.single().name
    }
  }

  private fun drain(): List<DeleteEvent> {
    val batch = mutableListOf<DeleteEvent>()
    while (true) {
      queue.poll()?.let { batch.add(it) } ?: break
    }
    return batch
  }

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

  fun pendingCount(): Int = queue.size

  companion object {
    private val logger: Logger =
      Logger.getLogger(BufferedDataAvailabilityCleanup::class.java.name)
    const val DEFAULT_BATCH_SIZE = 100
    const val DEFAULT_FLUSH_INTERVAL_SECONDS = 30L
    const val MAX_BATCH_DELETE_SIZE = 100
    private const val SHUTDOWN_TIMEOUT_SECONDS = 10L
    private const val BATCH_FLUSH_STATUS = "batch_flush"
    private const val LOG_INTERVAL = 50
  }
}
