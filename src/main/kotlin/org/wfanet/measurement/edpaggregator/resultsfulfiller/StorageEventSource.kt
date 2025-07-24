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

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.crypto.tink.KmsClient
import com.google.protobuf.Any
import com.google.protobuf.TypeRegistry
import java.time.LocalDate
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.edpaggregator.StorageConfig

/**
 * Event source implementation that reads events from storage using EventReader.
 *
 * This implementation wraps EventReader to provide events from storage,
 * conforming to the EventSource interface.
 *
 * @param eventReader The EventReader instance to use for reading events
 * @param dateRange The date range for which to read events
 * @param eventGroupReferenceIds The event group reference IDs to read
 * @param batchSize The size of event batches to return
 */
class StorageEventSource(
  private val eventReader: EventReader,
  private val dateRange: ClosedRange<LocalDate>,
  private val eventGroupReferenceIds: List<String>,
  private val batchSize: Int = DEFAULT_BATCH_SIZE
) : EventSource {

  companion object {
    private val logger = Logger.getLogger(StorageEventSource::class.java.name)
    const val DEFAULT_BATCH_SIZE = 256
  }

  /**
   * Secondary constructor that creates an EventReader internally.
   */
  constructor(
    kmsClient: KmsClient?,
    impressionsStorageConfig: StorageConfig,
    impressionDekStorageConfig: StorageConfig,
    labeledImpressionsDekPrefix: String,
    typeRegistry: TypeRegistry,
    dateRange: ClosedRange<LocalDate>,
    eventGroupReferenceIds: List<String>,
    batchSize: Int = DEFAULT_BATCH_SIZE
  ) : this(
    EventReader(
      kmsClient,
      impressionsStorageConfig,
      impressionDekStorageConfig,
      labeledImpressionsDekPrefix,
      typeRegistry,
      batchSize
    ),
    dateRange,
    eventGroupReferenceIds,
    batchSize
  )

  override suspend fun generateEventBatches(
    dispatcher: CoroutineContext
  ): Flow<List<LabeledEvent<Any>>> {
    logger.info("Starting storage-based event generation with batching")

    return channelFlow {
      // Process each date and event group combination in parallel
      val processingJobs = mutableListOf<kotlinx.coroutines.Job>()
      
      // Calculate total combinations for progress tracking
      val totalDays = java.time.temporal.ChronoUnit.DAYS.between(dateRange.start, dateRange.endInclusive) + 1
      val totalCombinations = totalDays * eventGroupReferenceIds.size
      var processedCombinations = 0L
      var totalEventsRead = 0L
      var totalBatchesSent = 0L

      logger.info("Processing $totalCombinations date/event group combinations (${totalDays} days × ${eventGroupReferenceIds.size} event groups)")

      var currentDate = dateRange.start
      while (currentDate <= dateRange.endInclusive) {
        val date = currentDate
        for (eventGroupReferenceId in eventGroupReferenceIds) {
          val job = launch(dispatcher) {
            logger.fine("Reading events for date $date and event group $eventGroupReferenceId")

            try {
              var batchCount = 0
              var eventCount = 0
              
              // Use the new batched API from EventReader  
              eventReader.getLabeledEventsBatched(date, eventGroupReferenceId, batchSize).collect { batch ->
                // Convert events to Any if needed
                @Suppress("UNCHECKED_CAST")
                val anyBatch = batch as List<LabeledEvent<Any>>
                send(anyBatch)
                batchCount++
                eventCount += batch.size
              }
              
              logger.fine("Read $eventCount events in $batchCount batches for date $date and event group $eventGroupReferenceId")
              
              // Update progress
              synchronized(this@channelFlow) {
                processedCombinations++
                totalEventsRead += eventCount
                totalBatchesSent += batchCount
                
                // Log progress every 10% or at least every 10 combinations
                if (processedCombinations % maxOf(1L, totalCombinations / 10) == 0L || processedCombinations == totalCombinations) {
                  val progressPercent = (processedCombinations * 100) / totalCombinations
                  logger.info("Progress: $progressPercent% ($processedCombinations/$totalCombinations combinations) - Total events: $totalEventsRead, Batches sent: $totalBatchesSent")
                }
              }
            } catch (e: ImpressionReadException) {
              logger.warning("Failed to read events for date $date and event group $eventGroupReferenceId: ${e.message}")
              // Continue with other dates/event groups
              synchronized(this@channelFlow) {
                processedCombinations++
              }
            }
          }

          processingJobs.add(job)
        }
        currentDate = currentDate.plusDays(1)
      }

      logger.info("Launched ${processingJobs.size} storage reading jobs")

      // Wait for all processing to complete
      processingJobs.forEach { it.join() }
      
      logger.info("Completed storage event generation - Total events: $totalEventsRead, Total batches: $totalBatchesSent")
    }
  }

  override suspend fun generateEvents(): Flow<LabeledEvent<Any>> {
    logger.info("Starting storage-based event generation")

    return kotlinx.coroutines.flow.flow {
      var currentDate = dateRange.start
      while (currentDate <= dateRange.endInclusive) {
        val date = currentDate
        for (eventGroupReferenceId in eventGroupReferenceIds) {
          try {
            eventReader.getLabeledEvents(date, eventGroupReferenceId).collect { event ->
              // Convert to Any if needed
              @Suppress("UNCHECKED_CAST")
              val anyEvent = event as LabeledEvent<Any>
              emit(anyEvent)
            }
          } catch (e: ImpressionReadException) {
            logger.warning("Failed to read events for date $date and event group $eventGroupReferenceId: ${e.message}")
            // Continue with other dates/event groups
          }
        }
        currentDate = currentDate.plusDays(1)
      }
    }
  }
}
