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

import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import io.opentelemetry.api.common.Attributes
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.locks.ReentrantLock
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.concurrent.withLock
import kotlin.time.TimeSource
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.batchDeleteImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.deleteImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataRequest
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.StorageClient

data class DeleteEvent(val deletedBlobPath: String, val impressionMetadataResourceId: String?)

/**
 * Buffers delete events in memory and flushes them via `BatchDeleteImpressionMetadata` RPCs, each
 * capped at [MAX_BATCH_DELETE_SIZE] records.
 *
 * On Cloud Run (Gen2 Cloud Functions), a single instance handles concurrent requests. When GCS
 * lifecycle rules delete many objects in a short window, the resulting OBJECT_DELETE events arrive
 * as concurrent HTTP requests to the same instance. This class accumulates those events and
 * processes them in batches, replacing N individual RPCs with ceil(N/100) batch RPCs.
 *
 * ## Flush strategy
 *
 * Every [enqueue] call attempts a synchronous flush. The flush lock serializes concurrent flushes:
 * the first thread to acquire the lock drains and processes all queued events; concurrent threads
 * block on the lock and then find the queue empty (their events were already flushed). This
 * naturally coalesces concurrent events into batches without any background timer.
 *
 * This design is critical on Cloud Run: background timer threads cannot reliably flush because
 * Cloud Run may scale down an instance as soon as all HTTP requests complete, killing background
 * threads before they fire. By flushing synchronously within the request, the HTTP connection stays
 * alive during the flush, keeping the instance active.
 *
 * ## Flush pipeline
 * 1. All blob paths are grouped by directory prefix and bulk-checked via `listBlobs` to find which
 *    blobs still have a live (current) version. Events whose blobs are still live are skipped — the
 *    delete event was for a noncurrent version and the object is still in use.
 * 2. Events without a resource ID are batch-resolved via a single `ListImpressionMetadata` RPC
 *    using the `blob_uris` filter.
 * 3. Resolved resource names are chunked into groups of [MAX_BATCH_DELETE_SIZE] and each chunk is
 *    deleted via a single `BatchDeleteImpressionMetadata` RPC.
 * 4. Events that fail resolution are re-queued for the next flush.
 */
class BufferedDataAvailabilityCleanup(
  private val impressionMetadataServiceStub: ImpressionMetadataServiceCoroutineStub,
  private val dataProviderName: String,
  private val storageClient: StorageClient,
  private val batchSize: Int = DEFAULT_BATCH_SIZE,
  @Suppress("UNUSED_PARAMETER") flushIntervalSeconds: Long = DEFAULT_FLUSH_INTERVAL_SECONDS,
  private val metrics: DataAvailabilityCleanupMetrics = DataAvailabilityCleanupMetrics(),
) {
  private val queue = ConcurrentLinkedQueue<DeleteEvent>()
  private val flushLock = ReentrantLock()

  fun enqueue(event: DeleteEvent) {
    queue.add(event)
    val size = queue.size
    if (size % LOG_INTERVAL == 0 || size <= 10) {
      logger.info("Buffered delete event for: ${event.deletedBlobPath} (queue size: $size)")
    }

    flush()
  }

  fun flush() {
    flushLock.withLock { doFlush() }
  }

  private fun doFlush() {
    val batch = drain()
    if (batch.isEmpty()) return

    val flushStart = TimeSource.Monotonic.markNow()
    logger.info("Flushing ${batch.size} buffered delete events")

    runBlocking {
      val checkStart = TimeSource.Monotonic.markNow()
      val liveBlobKeys = findLiveBlobKeys(batch)
      val checkMs = checkStart.elapsedNow().inWholeMilliseconds

      val resolvedEvents = mutableListOf<DeleteEvent>()
      var skippedCount = 0
      var resolveFailCount = 0
      val eventsNeedingResolution = mutableListOf<DeleteEvent>()

      for (event in batch) {
        try {
          val blobUri = SelectedStorageClient.parseBlobUri(event.deletedBlobPath)
          if (blobUri.key in liveBlobKeys) {
            skippedCount++
            continue
          }

          if (!event.impressionMetadataResourceId.isNullOrEmpty()) {
            resolvedEvents.add(event)
          } else {
            eventsNeedingResolution.add(event)
          }
        } catch (e: Exception) {
          logger.log(Level.WARNING, "Failed to check ${event.deletedBlobPath}, re-queuing", e)
          resolveFailCount++
          queue.add(event)
        }
      }
      logger.info(
        "Live-version check: ${resolvedEvents.size} pre-resolved, " +
          "${eventsNeedingResolution.size} need lookup, " +
          "$skippedCount skipped (live), $resolveFailCount failed " +
          "[${liveBlobKeys.size} live keys from ${batch.size} events, ${checkMs}ms]"
      )

      if (eventsNeedingResolution.isNotEmpty()) {
        val resolveStart = TimeSource.Monotonic.markNow()
        val resolved = batchResolveResourceNames(eventsNeedingResolution)
        val resolveMs = resolveStart.elapsedNow().inWholeMilliseconds
        logger.info(
          "Batch-resolved ${resolved.size}/${eventsNeedingResolution.size} " +
            "blob URIs in ${resolveMs}ms"
        )
        resolvedEvents.addAll(resolved)
        skippedCount += eventsNeedingResolution.size - resolved.size
      }

      if (resolvedEvents.isNotEmpty()) {
        val chunks = resolvedEvents.chunked(MAX_BATCH_DELETE_SIZE)
        logger.info(
          "Deleting ${resolvedEvents.size} records in ${chunks.size} batch RPC(s) " +
            "(max $MAX_BATCH_DELETE_SIZE per RPC)"
        )
        val deleteStart = TimeSource.Monotonic.markNow()
        var totalDeleted = 0

        for ((index, chunk) in chunks.withIndex()) {
          val chunkNames = chunk.map { it.impressionMetadataResourceId!! }
          try {
            impressionMetadataServiceStub.batchDeleteImpressionMetadata(
              batchDeleteImpressionMetadataRequest {
                parent = dataProviderName
                names += chunkNames
              }
            )
            totalDeleted += chunk.size
            logger.info("Batch RPC ${index + 1}/${chunks.size}: deleted ${chunk.size} records")
          } catch (e: Exception) {
            totalDeleted += handleBatchDeleteFailure(e, chunk, index + 1, chunks.size)
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
          Attributes.of(DataAvailabilityCleanupMetrics.CLEANUP_STATUS_ATTR, BATCH_FLUSH_STATUS),
        )
      } else {
        val totalMs = flushStart.elapsedNow().inWholeMilliseconds
        logger.info("No events to batch-delete ($skippedCount skipped) [total=${totalMs}ms]")
      }
    }
  }

  /**
   * Bulk-checks which blobs in [batch] still have a live (current) version in storage.
   *
   * Instead of N individual `getBlob` calls, this groups blob paths by their directory prefix and
   * issues one `listBlobs` per unique prefix. On a versioned GCS bucket, a delete event fires for
   * both noncurrent version deletions (where a newer current version may still exist) and final
   * deletions (no current version remains). Only the latter should trigger metadata cleanup.
   *
   * @return set of blob keys that still have a live version (should be skipped)
   */
  private suspend fun findLiveBlobKeys(batch: List<DeleteEvent>): Set<String> {
    val prefixes = mutableSetOf<String>()
    for (event in batch) {
      try {
        val blobUri = SelectedStorageClient.parseBlobUri(event.deletedBlobPath)
        val lastSlash = blobUri.key.lastIndexOf('/')
        if (lastSlash > 0) {
          prefixes.add(blobUri.key.substring(0, lastSlash + 1))
        }
      } catch (_: Exception) {}
    }

    val liveBlobKeys = mutableSetOf<String>()
    for (prefix in prefixes) {
      try {
        storageClient.listBlobs(prefix).toList().forEach { blob -> liveBlobKeys.add(blob.blobKey) }
      } catch (e: Exception) {
        logger.log(Level.WARNING, "Failed to list blobs with prefix $prefix", e)
      }
    }
    logger.info(
      "Bulk live-version check: ${prefixes.size} prefixes, ${liveBlobKeys.size} live blobs"
    )
    return liveBlobKeys
  }

  /**
   * Handles a failed `BatchDeleteImpressionMetadata` RPC.
   *
   * `BatchDeleteImpressionMetadata` is atomic — if ANY record in the batch returns NOT_FOUND, the
   * entire RPC fails with NOT_FOUND. When NOT_FOUND, falls back to individual deletes where
   * NOT_FOUND is treated as idempotent success. For other errors, re-queues the entire chunk using
   * the original [DeleteEvent] (preserving the blob path for the next flush cycle).
   *
   * @return the number of records successfully deleted (including NOT_FOUND as already-deleted)
   */
  private suspend fun handleBatchDeleteFailure(
    error: Exception,
    chunk: List<DeleteEvent>,
    rpcIndex: Int,
    totalRpcs: Int,
  ): Int {
    val statusCode = extractStatusCode(error)

    if (statusCode != Status.Code.NOT_FOUND) {
      logger.log(
        Level.SEVERE,
        "Batch RPC $rpcIndex/$totalRpcs failed for ${chunk.size} records, re-queuing",
        error,
      )
      chunk.forEach { event -> queue.add(event) }
      return 0
    }

    logger.warning(
      "Batch RPC $rpcIndex/$totalRpcs failed with NOT_FOUND for ${chunk.size} records. " +
        "Falling back to individual deletes."
    )

    var deleted = 0
    var alreadyGone = 0
    var failed = 0

    for (event in chunk) {
      try {
        impressionMetadataServiceStub.deleteImpressionMetadata(
          deleteImpressionMetadataRequest { name = event.impressionMetadataResourceId!! }
        )
        deleted++
      } catch (e: Exception) {
        val individualStatusCode = extractStatusCode(e)
        if (individualStatusCode == Status.Code.NOT_FOUND) {
          alreadyGone++
        } else {
          failed++
          logger.log(
            Level.WARNING,
            "Individual delete failed for ${event.impressionMetadataResourceId}, re-queuing",
            e,
          )
          queue.add(event)
        }
      }
    }

    logger.info(
      "Individual delete fallback: $deleted deleted, $alreadyGone already gone (NOT_FOUND), " +
        "$failed failed and re-queued"
    )
    return deleted + alreadyGone
  }

  private fun extractStatusCode(error: Exception): Status.Code? {
    return when (error) {
      is StatusRuntimeException -> error.status.code
      is StatusException -> error.status.code
      else -> null
    }
  }

  private suspend fun batchResolveResourceNames(events: List<DeleteEvent>): List<DeleteEvent> {
    val resolvedEvents = mutableListOf<DeleteEvent>()

    for (chunk in events.chunked(MAX_BATCH_RESOLVE_SIZE)) {
      val blobUris = chunk.map { it.deletedBlobPath }
      try {
        val listResponse =
          impressionMetadataServiceStub.listImpressionMetadata(
            listImpressionMetadataRequest {
              parent = dataProviderName
              filter = ListImpressionMetadataRequestKt.filter { this.blobUris += blobUris }
            }
          )

        val resultsByUri = listResponse.impressionMetadataList.groupBy { it.blobUri }
        for (event in chunk) {
          val matches = resultsByUri[event.deletedBlobPath]
          when {
            matches == null || matches.isEmpty() -> {
              logger.warning(
                "No ImpressionMetadata found for blob URI: ${event.deletedBlobPath}. Skipping."
              )
            }
            matches.size > 1 -> {
              logger.warning(
                "Multiple ImpressionMetadata records (${matches.size}) for " +
                  "${event.deletedBlobPath}. Skipping."
              )
            }
            else -> resolvedEvents.add(DeleteEvent(event.deletedBlobPath, matches.single().name))
          }
        }
      } catch (e: Exception) {
        logger.log(Level.WARNING, "Batch resolve failed for ${chunk.size} URIs, re-queuing", e)
        chunk.forEach { event -> queue.add(event) }
      }
    }

    return resolvedEvents
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
    private val logger: Logger = Logger.getLogger(BufferedDataAvailabilityCleanup::class.java.name)
    const val DEFAULT_BATCH_SIZE = 100
    const val DEFAULT_FLUSH_INTERVAL_SECONDS = 30L
    const val MAX_BATCH_DELETE_SIZE = 100
    const val MAX_BATCH_RESOLVE_SIZE = 100
    private const val BATCH_FLUSH_STATUS = "batch_flush"
    private const val LOG_INTERVAL = 50
  }
}
