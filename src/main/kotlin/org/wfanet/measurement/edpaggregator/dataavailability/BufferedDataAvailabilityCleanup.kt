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
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
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
 * Flushing is always done synchronously within the calling thread (the HTTP request handler
 * thread), avoiding CPU throttling issues with background threads on Cloud Functions Gen2
 * where CPU is throttled between invocations.
 *
 * Events are flushed when:
 * - The buffer reaches [batchSize] events (flushed by the enqueuing thread), OR
 * - The buffer has events older than [flushIntervalSeconds] (flushed by the next enqueuing
 *   thread).
 *
 * During flush:
 * 1. Events whose blobs still have a live version in storage are skipped (noncurrent version
 *    deletion).
 * 2. Events without a resource ID are batch-resolved via a single `ListImpressionMetadata` RPC
 *    using the `blob_uris` filter.
 * 3. Resolved resource names are chunked into groups of [MAX_BATCH_DELETE_SIZE] and each chunk
 *    is deleted via a single `BatchDeleteImpressionMetadata` RPC.
 * 4. Events that fail resolution are re-queued.
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
  private val firstEnqueueTimeMs = AtomicLong(0)
  private val flushing = AtomicBoolean(false)

  fun enqueue(event: DeleteEvent) {
    queue.add(event)
    firstEnqueueTimeMs.compareAndSet(0, System.currentTimeMillis())
    val size = queue.size
    if (size % LOG_INTERVAL == 0 || size <= 10) {
      logger.info(
        "Buffered delete event for: ${event.deletedBlobPath} (queue size: $size)"
      )
    }

    if (size >= batchSize) {
      logger.info("Batch size threshold ($batchSize) reached, flushing immediately")
      flush()
    } else if (isStale()) {
      logger.info("Buffer age exceeded ${flushIntervalSeconds}s, flushing")
      flush()
    }
  }

  private fun isStale(): Boolean {
    val first = firstEnqueueTimeMs.get()
    if (first == 0L) return false
    return System.currentTimeMillis() - first >= flushIntervalSeconds * 1000
  }

  fun flush() {
    if (!flushing.compareAndSet(false, true)) {
      return
    }
    try {
      doFlush()
    } finally {
      flushing.set(false)
    }
  }

  private fun doFlush() {
    val batch = drain()
    if (batch.isEmpty()) return

    firstEnqueueTimeMs.set(if (queue.isEmpty()) 0 else System.currentTimeMillis())
    val flushStart = TimeSource.Monotonic.markNow()
    logger.info("Flushing ${batch.size} buffered delete events")

    runBlocking {
      val resourceNames = mutableListOf<String>()
      var skippedCount = 0
      var resolveFailCount = 0
      val eventsNeedingResolution = mutableListOf<DeleteEvent>()

      val checkStart = TimeSource.Monotonic.markNow()
      for (event in batch) {
        try {
          if (!event.impressionMetadataResourceId.isNullOrEmpty()) {
            resourceNames.add(event.impressionMetadataResourceId)
            continue
          }

          val blobUri = SelectedStorageClient.parseBlobUri(event.deletedBlobPath)
          val liveBlob = storageClient.getBlob(blobUri.key)
          if (liveBlob != null) {
            skippedCount++
            continue
          }

          eventsNeedingResolution.add(event)
        } catch (e: Exception) {
          logger.log(
            Level.WARNING,
            "Failed to check ${event.deletedBlobPath}, re-queuing",
            e,
          )
          resolveFailCount++
          queue.add(event)
        }
      }
      val checkMs = checkStart.elapsedNow().inWholeMilliseconds
      logger.info(
        "Live-version check: ${resourceNames.size} pre-resolved, " +
          "${eventsNeedingResolution.size} need lookup, " +
          "$skippedCount skipped (live), $resolveFailCount failed [${checkMs}ms]"
      )

      if (eventsNeedingResolution.isNotEmpty()) {
        val resolveStart = TimeSource.Monotonic.markNow()
        val resolved = batchResolveResourceNames(eventsNeedingResolution)
        val resolveMs = resolveStart.elapsedNow().inWholeMilliseconds
        logger.info(
          "Batch-resolved ${resolved.size}/${eventsNeedingResolution.size} " +
            "blob URIs in ${resolveMs}ms"
        )
        resourceNames.addAll(resolved)
        skippedCount += eventsNeedingResolution.size - resolved.size
      }

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
              "Batch RPC ${index + 1}/${chunks.size} failed for ${chunk.size} records, " +
                "re-queuing",
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
            "[check=${checkMs}ms, delete=${deleteMs}ms, total=${totalMs}ms]"
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

  private suspend fun batchResolveResourceNames(
    events: List<DeleteEvent>,
  ): List<String> {
    val blobUris = events.map { it.deletedBlobPath }

    val resolvedNames = mutableListOf<String>()
    for (chunk in blobUris.chunked(MAX_BATCH_RESOLVE_SIZE)) {
      try {
        val listResponse =
          impressionMetadataServiceStub.listImpressionMetadata(
            listImpressionMetadataRequest {
              parent = dataProviderName
              filter = ListImpressionMetadataRequestKt.filter {
                this.blobUris += chunk
              }
            }
          )

        val resultsByUri = listResponse.impressionMetadataList.groupBy { it.blobUri }
        for (uri in chunk) {
          val matches = resultsByUri[uri]
          when {
            matches == null || matches.isEmpty() -> {
              logger.warning("No ImpressionMetadata found for blob URI: $uri. Skipping.")
            }
            matches.size > 1 -> {
              logger.warning(
                "Multiple ImpressionMetadata records (${matches.size}) for $uri. Skipping."
              )
            }
            else -> resolvedNames.add(matches.single().name)
          }
        }
      } catch (e: Exception) {
        logger.log(
          Level.WARNING,
          "Batch resolve failed for ${chunk.size} URIs, re-queuing",
          e,
        )
        chunk.forEach { uri -> queue.add(DeleteEvent(uri, null)) }
      }
    }

    return resolvedNames
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
  }

  fun pendingCount(): Int = queue.size

  companion object {
    private val logger: Logger =
      Logger.getLogger(BufferedDataAvailabilityCleanup::class.java.name)
    const val DEFAULT_BATCH_SIZE = 100
    const val DEFAULT_FLUSH_INTERVAL_SECONDS = 30L
    const val MAX_BATCH_DELETE_SIZE = 100
    const val MAX_BATCH_RESOLVE_SIZE = 100
    private const val BATCH_FLUSH_STATUS = "batch_flush"
    private const val LOG_INTERVAL = 50
  }
}
