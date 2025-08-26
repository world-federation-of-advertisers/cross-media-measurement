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

import com.google.protobuf.Descriptors
import com.google.protobuf.Message
import java.time.Instant
import java.util.logging.Logger
import org.projectnessie.cel.Program
import org.projectnessie.cel.common.types.BoolT
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters

/**
 * Filter processor for event filtering with CEL expressions and time ranges.
 *
 * This processor compiles a CEL expression once and reuses it for batch processing.
 * It processes events sequentially through the CEL filter and identifies matching events.
 * The processor supports filtering by event group reference ID, time ranges, CEL expressions.
 *
 * @param filterSpec immutable specification containing all filtering criteria
 */
class FilterProcessor(
  val filterSpec: FilterSpec,
  val eventDescriptor: Descriptors.Descriptor
) {

  companion object {
    private val logger = Logger.getLogger(FilterProcessor::class.java.name)
  }

  /**
   * Compiled CEL program for event filtering.
   */
  private val program: Program = if (filterSpec.celExpression.isEmpty()) {
    logger.fine { "No CEL expression provided, using always-true filter" }
    Program { Program.newEvalResult(BoolT.True, null) }
  } else {
    logger.fine { "Compiling CEL expression: ${filterSpec.celExpression}" }
    EventFilters.compileProgram(eventDescriptor, filterSpec.celExpression)
  }

  /**
   * Start time instant from the collection interval.
   *
   * Pre-computed to avoid repeated conversion from protobuf Timestamp to Java Instant
   * during batch processing.
   */
  private val startInstant: Instant =
    Instant.ofEpochSecond(
      filterSpec.collectionInterval.startTime.seconds,
      filterSpec.collectionInterval.startTime.nanos.toLong()
    )

  /**
   * End time instant from the collection interval.
   *
   * Pre-computed to avoid repeated conversion from protobuf Timestamp to Java Instant
   * during batch processing.
   */
  private val endInstant: Instant =
    Instant.ofEpochSecond(
      filterSpec.collectionInterval.endTime.seconds,
      filterSpec.collectionInterval.endTime.nanos.toLong()
    )

  /**
   * Processes a batch of events and returns the matching events.
   *
   * Applies filtering in the following order for optimal performance:
   * 1. Batch time range overlap check (fastest, avoids processing entire batch)
   * 2. Event group reference ID filtering (fast, string comparison)
   * 3. Time range filtering (fast, cached instant comparisons)
   * 4. CEL expression filtering (slower, requires expression evaluation)
   *
   * @param batch The batch of events to process. Must contain a valid list of events.
   * @return An EventBatch containing events that match all configured filters. The batch may be empty
   *   if no events match the criteria.
   *
   * @throws Exception if there are issues with event processing, though individual
   *   event failures are logged and do not stop batch processing.
   */
  fun processBatch(batch: EventBatch): EventBatch {
    if (batch.events.isEmpty()) {
      logger.finest { "Skipping empty batch" }
      return batch
    }

    logger.fine { "Processing batch with ${batch.events.size} events" }

    // Fast batch-level time range check: skip entire batch if no overlap
    if (!batchTimeRangeOverlaps(batch)) {
      logger.fine { "Batch time range [${batch.minTime}, ${batch.maxTime}] does not overlap with filter interval [$startInstant, $endInstant)" }
      return batch.copy(events = emptyList())
    }

    var eventsFilteredByGroup = 0
    var eventsFilteredByTime = 0
    var eventsFilteredByCel = 0

    val filteredEvents = batch.events.filter { event ->
      if (!filterSpec.eventGroupReferenceIds.contains(event.eventGroupReferenceId)) {
        eventsFilteredByGroup++
        logger.finest { "Event filtered out by group reference ID: ${event.eventGroupReferenceId}" }
        return@filter false
      }

      if (!isEventInTimeRange(event)) {
        eventsFilteredByTime++
        logger.finest { "Event filtered out by time range: ${event.timestamp}" }
        return@filter false
      }

      val celMatch = EventFilters.matches(event.message, program)
      if (!celMatch) {
        eventsFilteredByCel++
        logger.finest { "Event filtered out by CEL expression" }
      }
      celMatch
    }

    logger.fine { 
      "Batch processing complete: ${filteredEvents.size}/${batch.events.size} events passed filters " +
      "(filtered by group: $eventsFilteredByGroup, by time: $eventsFilteredByTime, by CEL: $eventsFilteredByCel)"
    }

    return batch.copy(events = filteredEvents)
  }

  /**
   * Checks if the batch's time range overlaps with the filter's collection interval.
   *
   * This is a fast batch-level check to avoid processing events when the entire batch
   * is outside the collection interval.
   *
   * @param batch the batch to check
   * @return `true` if the batch time range overlaps with the collection interval, `false` otherwise
   */
  private fun batchTimeRangeOverlaps(batch: EventBatch): Boolean {
    // Check if batch [minTime, maxTime] overlaps with filter [startInstant, endInstant)
    // For the collection interval [start, end), events at exactly start time should be included
    // Overlap exists if: batch.maxTime >= startInstant AND batch.minTime < endInstant
    val overlaps = !batch.maxTime.isBefore(startInstant) && batch.minTime.isBefore(endInstant)
    logger.finest { 
      "Batch time overlap check: batch[${batch.minTime}, ${batch.maxTime}] vs filter[$startInstant, $endInstant) = $overlaps"
    }
    return overlaps
  }

  /**
   * Checks if an event's timestamp falls within the collection interval.
   *
   * Uses a half-open interval [start, end) where the start time is inclusive
   * and the end time is exclusive. This follows standard time interval conventions.
   *
   * @param event the event to check
   * @return `true` if the event timestamp is within the interval, `false` otherwise
   */
  private fun isEventInTimeRange(event: LabeledEvent<Message>): Boolean {
    val eventTime = event.timestamp
    return !eventTime.isBefore(startInstant) && eventTime.isBefore(endInstant)
  }
}
