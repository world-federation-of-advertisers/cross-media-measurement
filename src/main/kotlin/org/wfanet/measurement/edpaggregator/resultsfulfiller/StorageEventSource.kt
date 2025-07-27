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
import java.time.LocalDate
import java.time.ZoneId
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions.EventGroupDetails

/**
 * Creates [EventReader] instances for specific sources.
 *
 * Used by [StorageEventSource] to decouple reader construction from source resolution and to
 * support testing.
 */
interface EventReaderFactory {
  /** Creates a reader for resolved [BlobDetails]. */
  fun createEventReader(blobDetails: BlobDetails): EventReader
}

/** Default implementation of EventReaderFactory that creates StorageEventReader instances. */
class DefaultEventReaderFactory(
  private val kmsClient: KmsClient?,
  private val impressionsStorageConfig: StorageConfig,
  private val descriptor: Descriptors.Descriptor,
  private val batchSize: Int,
) : EventReaderFactory {
  override fun createEventReader(blobDetails: BlobDetails): EventReader {
    return StorageEventReader(
      blobDetails = blobDetails,
      kmsClient = kmsClient,
      impressionsStorageConfig = impressionsStorageConfig,
      descriptor = descriptor,
      batchSize = batchSize,
    )
  }
}

/** Component for tracking progress during event processing. */
class ProgressTracker(private val totalEventReaders: Int) {
  private var processedReaders = 0L
  private var totalEventsRead = 0L
  private var totalBatchesSent = 0L

  fun updateProgress(eventCount: Int, batchCount: Int) {
    synchronized(this) {
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

  fun logCompletion() {
    logger.info(
      "Completed storage event generation - Total events: $totalEventsRead, Total batches: $totalBatchesSent"
    )
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
 * @property impressionMetadataService facade providing data sources for intervals.
 * @property eventReaderFactory factory for constructing [EventReader]s.
 * @property eventGroupDetailsList event groups with their collection intervals.
 */
class StorageEventSource(
  private val impressionMetadataService: ImpressionMetadataService,
  private val eventReaderFactory: EventReaderFactory,
  private val eventGroupDetailsList: List<EventGroupDetails>,
  private val zoneId: ZoneId,
) : EventSource {

  companion object {
    private val logger = Logger.getLogger(StorageEventSource::class.java.name)
  }

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
   * @param dispatcher The coroutine context to use for parallel processing
   * @return A [Flow] that emits [EventBatch]es as they are read from storage
   */
  override suspend fun generateEventBatches(dispatcher: CoroutineContext): Flow<EventBatch> {
    logger.info("Starting storage-based event generation with batching")

    return channelFlow {
      val eventReaders = createEventReaders()
      val progressTracker = ProgressTracker(eventReaders.size)

      logger.info(
        "Processing ${eventReaders.size} EventReaders across ${eventGroupDetailsList.size} event groups"
      )

      // Launch one coroutine per EventReader
      val processingJobs =
        eventReaders.map { readerInfo ->
          launch(dispatcher) {
            val (batchCount, eventCount) =
              processEventReader(readerInfo) { eventBatch -> send(eventBatch) }
            progressTracker.updateProgress(eventCount, batchCount)
          }
        }

      logger.info("Launched ${processingJobs.size} EventReader processing coroutines")

      // Wait for all event readers to complete
      processingJobs.joinAll()
      progressTracker.logCompletion()
    }
  }

  /** Creates EventReader instances for all data sources provided by the metadata service. */
  private suspend fun createEventReaders(): List<EventReaderInfo> {
    val allSources = mutableListOf<Pair<String, ImpressionDataSource>>()
    eventGroupDetailsList.forEach { details ->
      details.collectionIntervalsList.forEach { interval ->
        val sources =
          impressionMetadataService.listImpressionDataSources(
            details.eventGroupReferenceId,
            interval,
          )
        sources.forEach { src -> allSources += details.eventGroupReferenceId to src }
      }
    }

    // Deduplicate sources by blob URI to avoid double-reading overlapping intervals.
    val uniqueSources = allSources.distinctBy { it.second.blobDetails.blobUri }

    return uniqueSources.map { (eventGroupReferenceId, source) ->
      val eventReader = eventReaderFactory.createEventReader(source.blobDetails)
      val date = LocalDate.ofInstant(source.interval.startTime.toInstant(), zoneId)
      EventReaderInfo(
        eventReader = eventReader,
        eventGroupReferenceId = eventGroupReferenceId,
        date = date,
      )
    }
  }

  /** Processes events from a single EventReader and returns batch and event counts. */
  private suspend fun processEventReader(
    readerInfo: EventReaderInfo,
    sendEventBatch: suspend (EventBatch) -> Unit,
  ): Pair<Int, Int> {
    var batchCount = 0
    var eventCount = 0

    logger.fine(
      "Reading events for event group ${readerInfo.eventGroupReferenceId} on date ${readerInfo.date}"
    )

    readerInfo.eventReader.readEvents().collect { events ->
      val eventBatch =
        EventBatch(
          events = events,
          minTime = events.minOf { it.timestamp },
          maxTime = events.maxOf { it.timestamp },
        )
      sendEventBatch(eventBatch)
      batchCount++
      eventCount += events.size
    }

    logger.fine(
      "Read $eventCount events in $batchCount batches for event group ${readerInfo.eventGroupReferenceId} on date ${readerInfo.date}"
    )
    return Pair(batchCount, eventCount)
  }

  /** Holds an EventReader instance with its associated metadata. */
  private data class EventReaderInfo(
    val eventReader: EventReader,
    val eventGroupReferenceId: String,
    val date: LocalDate,
  )
}
