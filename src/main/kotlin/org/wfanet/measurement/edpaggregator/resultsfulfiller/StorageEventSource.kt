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
import com.google.protobuf.DynamicMessage
import com.google.protobuf.Message
import com.google.protobuf.TypeRegistry
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions.EventGroupDetails

/**
 * Factory interface for creating EventReader instances.
 * This abstraction enables easy mocking and testing of the StorageEventSource.
 */
interface EventReaderFactory {
  /**
   * Creates an EventReader for the specified blob and metadata paths.
   */
  fun createEventReader(
    blobPath: String,
    metadataPath: String
  ): EventReader
}

/**
 * Default implementation of EventReaderFactory that creates StorageEventReader instances.
 */
class DefaultEventReaderFactory(
  private val kmsClient: KmsClient?,
  private val impressionsStorageConfig: StorageConfig,
  private val impressionDekStorageConfig: StorageConfig,
  private val typeRegistry: TypeRegistry,
  private val batchSize: Int
) : EventReaderFactory {
  override fun createEventReader(
    blobPath: String,
    metadataPath: String
  ): EventReader {
    return StorageEventReader(
      blobPath = blobPath,
      metadataPath = metadataPath,
      kmsClient = kmsClient,
      impressionsStorageConfig = impressionsStorageConfig,
      impressionDekStorageConfig = impressionDekStorageConfig,
      typeRegistry = typeRegistry,
      batchSize = batchSize
    )
  }
}

/**
 * Data class representing date ranges for event group processing.
 */
data class EventGroupDateRange(
  val eventGroupReferenceId: String,
  val startDate: LocalDate,
  val endDate: LocalDate
)

/**
 * Component for tracking progress during event processing.
 */
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
      if (processedReaders % maxOf(1L, totalEventReaders.toLong() / 10) == 0L || 
          processedReaders == totalEventReaders.toLong()) {
        val progressPercent = (processedReaders * 100) / totalEventReaders
        logger.info("Progress: $progressPercent% ($processedReaders/$totalEventReaders EventReaders) - Total events: $totalEventsRead, Batches sent: $totalBatchesSent")
      }
    }
  }
  
  fun logCompletion() {
    logger.info("Completed storage event generation - Total events: $totalEventsRead, Total batches: $totalBatchesSent")
  }
  
  companion object {
    private val logger = Logger.getLogger(ProgressTracker::class.java.name)
  }
}

/**
 * Storage-based implementation of [EventSource] that reads events from blob storage.
 *
 * This class provides parallel event reading from storage by creating individual [EventReader]
 * instances for each date/event group combination. It processes events from multiple event groups
 * and time intervals concurrently, streaming results as they become available.
 *
 * ## Event Organization
 *
 * Events are organized in storage by:
 * - Event group reference ID
 * - Date (events are partitioned by day)
 *
 * Each blob contains events for a specific event group on a specific date.
 *
 * ## Performance Characteristics
 *
 * - **Parallelism**: One coroutine per date/event group combination
 * - **Memory usage**: Bounded by batch size and number of concurrent readers
 *
 * @property pathResolver Resolves storage paths for event blobs and metadata
 * @property eventReaderFactory Factory for creating EventReader instances
 * @property eventGroupDetailsList List of event groups with their collection time intervals
 */
class StorageEventSource(
  private val pathResolver: EventPathResolver,
  private val eventReaderFactory: EventReaderFactory,
  private val eventGroupDetailsList: List<EventGroupDetails>
) : EventSource {

  companion object {
    private val logger = Logger.getLogger(StorageEventSource::class.java.name)

    /** Default batch size for event reading operations. */
    const val DEFAULT_BATCH_SIZE = 256
    
    /**
     * Expands event group details into individual date ranges.
     * 
     * This function processes each event group's collection intervals and expands them
     * into individual date ranges, deduplicating any overlapping periods.
     * 
     * @param eventGroupDetailsList List of event groups with their collection intervals
     * @return List of unique date ranges for each event group
     */
    fun expandEventGroupDateRanges(
      eventGroupDetailsList: List<EventGroupDetails>
    ): List<EventGroupDateRange> {
      return eventGroupDetailsList
        .flatMap { eventGroupDetails ->
          eventGroupDetails.collectionIntervalsList.flatMap { interval ->
            val startDate = interval.startTime.toLocalDate()
            val endDate = interval.endTime.toLocalDate()
            
            generateSequence(startDate) { date -> 
              if (date < endDate) date.plusDays(1) else null 
            }.map { date ->
              EventGroupDateRange(
                eventGroupReferenceId = eventGroupDetails.eventGroupReferenceId,
                startDate = date,
                endDate = date
              )
            }.toList()
          }
        }
        .distinctBy { Pair(it.eventGroupReferenceId, it.startDate) }
    }
    
    /**
     * Extension function to convert protobuf Timestamp to LocalDate.
     */
    private fun com.google.protobuf.Timestamp.toLocalDate(): LocalDate {
      return Instant.ofEpochSecond(this.seconds, this.nanos.toLong())
        .atZone(ZoneOffset.UTC)
        .toLocalDate()
    }
  }

  /**
   * Creates a StorageEventSource with default implementations.
   *
   * This constructor creates a [DefaultEventPathResolver] and [DefaultEventReaderFactory]
   * internally using the provided configuration. Use this when you don't need custom
   * path resolution or event reader creation logic.
   *
   * @param kmsClient Client for Key Management Service operations (nullable for unencrypted storage)
   * @param impressionsStorageConfig Storage configuration for impression data blobs
   * @param impressionDekStorageConfig Storage configuration for Data Encryption Keys
   * @param impressionsBucketUri Base URI for the impressions storage bucket
   * @param impressionsDekBucketUri Base URI for the impression DEK storage bucket
   * @param typeRegistry Registry for deserializing protocol buffer message types
   * @param eventGroupDetailsList List of event groups with their collection time intervals
   * @param batchSize Maximum number of events to return in each batch (default: 256)
   */
  constructor(
    kmsClient: KmsClient?,
    impressionsStorageConfig: StorageConfig,
    impressionDekStorageConfig: StorageConfig,
    impressionsBucketUri: String,
    impressionsDekBucketUri: String,
    typeRegistry: TypeRegistry,
    eventGroupDetailsList: List<EventGroupDetails>,
    batchSize: Int = DEFAULT_BATCH_SIZE
  ) : this(
    DefaultEventPathResolver(
      impressionsBucketUri = impressionsBucketUri,
      impressionsDekBucketUri = impressionsDekBucketUri
    ),
    DefaultEventReaderFactory(
      kmsClient = kmsClient,
      impressionsStorageConfig = impressionsStorageConfig,
      impressionDekStorageConfig = impressionDekStorageConfig,
      typeRegistry = typeRegistry,
      batchSize = batchSize
    ),
    eventGroupDetailsList
  )

  /**
   * Generates batches of events by reading from storage in parallel.
   *
   * This method creates an [EventReader] for each combination of event group and date
   * within the specified collection intervals. All readers operate concurrently to
   * maximize throughput.
   *
   * ## Implementation Details
   *
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
  override suspend fun generateEventBatches(
    dispatcher: CoroutineContext
  ): Flow<EventBatch> {
    logger.info("Starting storage-based event generation with batching")

    return channelFlow {
      val eventReaders = createEventReaders()
      val progressTracker = ProgressTracker(eventReaders.size)
      var batchIdCounter = 0L

      logger.info("Processing ${eventReaders.size} EventReaders across ${eventGroupDetailsList.size} event groups")

      // Launch one coroutine per EventReader
      val processingJobs = eventReaders.map { readerInfo ->
        launch(dispatcher) {
          val (batchCount, eventCount) = processEventReader(readerInfo) { eventBatch ->
            val batchWithId = eventBatch.copy(
              batchId = synchronized(this@channelFlow) { ++batchIdCounter }
            )
            send(batchWithId)
          }

          // Update progress
          progressTracker.updateProgress(eventCount, batchCount)
        }
      }

      logger.info("Launched ${processingJobs.size} EventReader processing coroutines")

      // Wait for all processing to complete
      processingJobs.joinAll()
      progressTracker.logCompletion()
    }
  }

  /**
   * Creates EventReader instances for all event group and date combinations.
   */
  private suspend fun createEventReaders(): List<EventReaderInfo> {
    val dateRanges = StorageEventSource.expandEventGroupDateRanges(eventGroupDetailsList)
    return dateRanges.map { dateRange: EventGroupDateRange ->
      val paths = pathResolver.resolvePaths(
        dateRange.startDate, 
        dateRange.eventGroupReferenceId
      )
      val eventReader = eventReaderFactory.createEventReader(
        blobPath = paths.blobPath,
        metadataPath = paths.metadataPath
      )
      EventReaderInfo(
        eventReader = eventReader,
        eventGroupReferenceId = dateRange.eventGroupReferenceId,
        date = dateRange.startDate
      )
    }
  }

  /**
   * Processes events from a single EventReader and returns batch and event counts.
   */
  private suspend fun processEventReader(
    readerInfo: EventReaderInfo,
    sendEventBatch: suspend (EventBatch) -> Unit
  ): Pair<Int, Int> {
    var batchCount = 0
    var eventCount = 0

    logger.fine("Reading events for event group ${readerInfo.eventGroupReferenceId} on date ${readerInfo.date}")

    readerInfo.eventReader.readEvents().collect { events ->
      val eventBatch = EventBatch(
        events = events,
        batchId = 0 // Will be set by caller
      )
      sendEventBatch(eventBatch)
      batchCount++
      eventCount += events.size
    }

    logger.fine("Read $eventCount events in $batchCount batches for event group ${readerInfo.eventGroupReferenceId} on date ${readerInfo.date}")
    return Pair(batchCount, eventCount)
  }

  /**
   * Holds an EventReader instance with its associated metadata.
   */
  private data class EventReaderInfo(
    val eventReader: EventReader,
    val eventGroupReferenceId: String,
    val date: LocalDate
  )
}
