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

/**
 * Storage-based event source implementation for reading events from blob storage.
 *
 * This file contains the [StorageEventSource] class which provides parallel event reading
 * capabilities for the EDP aggregator results fulfiller system.
 */
package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.crypto.tink.KmsClient
import com.google.protobuf.Descriptors
import com.google.protobuf.Message
import java.util.logging.Logger
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.launch
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions.EventGroupDetails

/** Result of processing a single EventReader. */
data class EventReaderResult(val batchCount: Int, val eventCount: Int)

/** Component for tracking progress during event processing. */
class ProgressTracker(private val totalEventReaders: Int) : AutoCloseable {
  private var processedReaders = 0L
  private var totalEventsRead = 0L
  private var totalBatchesSent = 0L
  private var closed = false

  fun updateProgress(eventCount: Int, batchCount: Int) {
    synchronized(this) {
      check(!closed) { "ProgressTracker has been closed and cannot be reused" }

      processedReaders++
      totalEventsRead += eventCount
      totalBatchesSent += batchCount

      // Log progress every 10% or at least every 10 readers
      if (
        processedReaders % maxOf(1L, totalEventReaders.toLong() / 10) == 0L ||
          processedReaders == totalEventReaders.toLong()
      ) {
        val progressPercent = (processedReaders * 100) / totalEventReaders
        logger.info(
          "Progress: $progressPercent% ($processedReaders/$totalEventReaders EventReaders) - Total events: $totalEventsRead, Batches sent: $totalBatchesSent"
        )
      }
    }
  }

  override fun close() {
    synchronized(this) {
      if (!closed) {
        closed = true
        logger.info(
          "Completed storage event generation - Total events: $totalEventsRead, Total batches: $totalBatchesSent"
        )
      }
    }
  }

  companion object {
    private val logger = Logger.getLogger(ProgressTracker::class.java.name)
  }
}

/**
 * Event source that builds readers from an impression metadata facade.
 *
 * Resolves data sources via [ImpressionMetadataService] for each configured event group interval,
 * then creates [EventReader]s and streams batches in parallel. This class is agnostic to storage
 * layout and transport.
 *
 * Performance characteristics:
 * - Parallelism: one coroutine per resolved data source
 * - Memory: bounded by batch size and number of concurrent readers
 *
 * @property impressionDataSourceProvider facade providing data sources for intervals.
 * @property eventGroupDetailsList event groups with their collection intervals.
 * @property modelLine model line to use for fetching impressions
 * @property kmsClient KMS client for decryption operations
 * @property impressionsStorageConfig storage configuration for reading impressions
 * @property descriptor protobuf descriptor for parsing events
 * @property batchSize batch size for event reading
 */
class StorageEventSource(
  private val impressionDataSourceProvider: ImpressionDataSourceProvider,
  private val eventGroupDetailsList: List<EventGroupDetails>,
  private val modelLine: String,
  private val kmsClient: KmsClient?,
  private val impressionsStorageConfig: StorageConfig,
  private val descriptor: Descriptors.Descriptor,
  private val batchSize: Int,
) : EventSource<Message> {

  /** Cache for unique impression data sources to avoid duplicate API calls. */
  private var cachedImpressionDataSources: List<ImpressionDataSource>? = null

  /**
   * Generates batches of events by reading from storage in parallel.
   *
   * This method creates an [EventReader] for each combination of event group and date within the
   * specified collection intervals. All readers operate concurrently to maximize throughput.
   *
   * ## Implementation Details
   * 1. **Expansion**: Expands each event group's collection intervals into individual dates
   * 2. **Reader creation**: Creates one [EventReader] per date/event group combination
   * 3. **Parallel execution**: Launches all readers concurrently using the provided dispatcher
   * 4. **Streaming**: Results are streamed back as they become available via a channel flow
   *
   * ## Error Handling
   *
   * If any individual [EventReader] fails:
   * - The entire operation fails fast
   * - All other concurrent readers are cancelled
   * - The original exception is propagated to the caller
   *
   * ## Progress Tracking
   *
   * Progress is logged at:
   * - Every 10% of completion
   * - Upon full completion with total events and batches processed
   *
   * @return A [Flow] that emits [EventBatch]es as they are read from storage
   */
  override fun generateEventBatches(): Flow<EventBatch<Message>> {
    logger.info("Starting storage-based event generation with batching")

    return channelFlow {
      val eventReaders: List<StorageEventReader> = createEventReaders()
      ProgressTracker(eventReaders.size).use { progressTracker ->
        logger.info(
          "Processing ${eventReaders.size} EventReaders across ${eventGroupDetailsList.size} event groups"
        )
        // Launch one coroutine per EventReader
        coroutineScope {
          eventReaders.forEach { eventReader ->
            launch {
              val result = processEventReader(eventReader) { eventBatch -> send(eventBatch) }
              progressTracker.updateProgress(result.eventCount, result.batchCount)
            }
          }
          logger.info("Launched ${eventReaders.size} EventReader processing coroutines")
        }
      }
    }
  }

  /** Collects all impression data sources and deduplicates by blob URI. */
  private suspend fun getUniqueImpressionDataSources(): List<ImpressionDataSource> {
    cachedImpressionDataSources?.let { return it }

    val allSources =
      eventGroupDetailsList.flatMap { details ->
        logger.info("EventGroup details: $details")
        details.collectionIntervalsList.flatMap { interval ->
          logger.info("EventGroup collection interval: $interval")
          impressionDataSourceProvider.listImpressionDataSources(
            modelLine,
            details.eventGroupReferenceId,
            interval,
          )
        }
      }
    val result = allSources.distinctBy { it.blobDetails.blobUri }
    cachedImpressionDataSources = result
    return result
  }

  private suspend fun createEventReaders(): List<StorageEventReader> {
    logger.info("Creating event readers... ")
    val uniqueSources = getUniqueImpressionDataSources()

    return uniqueSources.map { source ->
      StorageEventReader(
        source.blobDetails,
        kmsClient,
        impressionsStorageConfig,
        descriptor,
        batchSize,
      )
    }
  }

  /** Processes events from a single EventReader and returns batch and event counts. */
  private suspend fun processEventReader(
    eventReader: StorageEventReader,
    sendEventBatch: suspend (EventBatch<Message>) -> Unit,
  ): EventReaderResult {
    var batchCount = 0
    var eventCount = 0
    val blobDetails = eventReader.getBlobDetails()

    logger.fine("Reading events from ${blobDetails.blobUri}")

    eventReader.readEvents().collect { events ->
      val eventBatch =
        EventBatch(
          events = events,
          minTime = events.minOf { it.timestamp },
          maxTime = events.maxOf { it.timestamp },
          eventGroupReferenceId = blobDetails.eventGroupReferenceId,
        )
      sendEventBatch(eventBatch)
      batchCount++
      eventCount += events.size
    }

    logger.fine("Read $eventCount events in $batchCount batches for ${blobDetails.blobUri}")
    return EventReaderResult(batchCount, eventCount)
  }

  /**
   * Gets the KEK URI from the impression data sources.
   *
   * Returns the KEK URI from the most recent data source (sorted by interval end time in descending
   * order). All data sources for the same EDP must use KEK URIs with the same project ID and
   * location.
   *
   * Returns null if no data sources are available, in which case a non-encrypted empty sketch will
   * be fulfilled.
   *
   * @throws IllegalArgumentException if KEK URIs have different project IDs or locations
   */
  suspend fun getKekUri(): String? {
    val uniqueSources = getUniqueImpressionDataSources()
    if (uniqueSources.isEmpty()) return null

    // Validate all KEK URIs have the same project ID and location
    val kekUris = uniqueSources.map { it.blobDetails.encryptedDek.kekUri }
    validateKekUrisConsistency(kekUris)

    // Sort by interval end time in descending order and return the first KEK URI
    val sortedSources = uniqueSources.sortedByDescending { it.interval.endTime.seconds }
    return sortedSources.first().blobDetails.encryptedDek.kekUri
  }

  /**
   * Validates that all KEK URIs have the same project ID and location.
   *
   * KEK URI format:
   * gcp-kms://projects/{PROJECT_ID}/locations/{LOCATION}/keyRings/{KEY_RING}/cryptoKeys/{KEY_NAME}
   */
  private fun validateKekUrisConsistency(kekUris: List<String>) {
    if (kekUris.size <= 1) return

    val firstProjectAndLocation = extractProjectAndLocation(kekUris.first())
    for (kekUri in kekUris.drop(1)) {
      val projectAndLocation = extractProjectAndLocation(kekUri)
      require(projectAndLocation == firstProjectAndLocation) {
        "All KEK URIs must have the same project ID and location. " +
          "Expected: $firstProjectAndLocation, Found: $projectAndLocation in URI: $kekUri"
      }
    }
  }

  /**
   * Extracts the project ID and location from a KEK URI.
   *
   * For GCP KMS URIs (gcp-kms://projects/{PROJECT}/locations/{LOCATION}/...), returns the actual
   * project ID and location. For non-GCP URIs (e.g., fake-kms:// used in tests), returns empty
   * strings which allows consistency checking to still work (all non-GCP URIs are considered to
   * have the same "empty" project/location).
   *
   * @return Pair of (projectId, location), or empty strings for non-GCP URIs
   */
  private fun extractProjectAndLocation(kekUri: String): Pair<String, String> {
    // For non-GCP-KMS URIs (e.g., fake-kms:// used in tests), skip project/location extraction
    if (!kekUri.startsWith("gcp-kms://")) {
      return Pair("", "")
    }
    val regex =
      Regex("gcp-kms://projects/([^/]+)/locations/([^/]+)/keyRings/[^/]+/cryptoKeys/[^/]+")
    val matchResult = regex.matchEntire(kekUri)
    requireNotNull(matchResult) { "Invalid GCP KMS KEK URI format: $kekUri" }
    return Pair(matchResult.groupValues[1], matchResult.groupValues[2])
  }

  companion object {
    private val logger = Logger.getLogger(StorageEventSource::class.java.name)
  }
}
