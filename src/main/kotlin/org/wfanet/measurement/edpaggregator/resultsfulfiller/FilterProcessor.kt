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
import org.projectnessie.cel.Program
import org.projectnessie.cel.common.types.BoolT
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters

/**
 * Filter processor for event filtering with CEL expressions and time ranges.
 *
 * This processor compiles a CEL expression once and reuses it for batch processing. It processes
 * events sequentially through the CEL filter and identifies matching events. The processor supports
 * filtering by event group reference ID, time ranges, CEL expressions.
 *
 * @param filterSpec immutable specification containing all filtering criteria
 */
class FilterProcessor<T : Message>(
  val filterSpec: FilterSpec,
  val eventDescriptor: Descriptors.Descriptor,
) {

  /** Compiled CEL program for event filtering. */
  private val program: Program =
    if (filterSpec.celExpression.isEmpty()) {
      Program { Program.newEvalResult(BoolT.True, null) }
    } else {
      EventFilters.compileProgram(eventDescriptor, filterSpec.celExpression)
    }

  /**
   * Start time instant from the collection interval.
   *
   * Pre-computed to avoid repeated conversion from protobuf Timestamp to Java Instant during batch
   * processing.
   */
  private val startInstant: Instant =
    Instant.ofEpochSecond(
      filterSpec.collectionInterval.startTime.seconds,
      filterSpec.collectionInterval.startTime.nanos.toLong(),
    )

  /**
   * End time instant from the collection interval.
   *
   * Pre-computed to avoid repeated conversion from protobuf Timestamp to Java Instant during batch
   * processing.
   */
  private val endInstant: Instant =
    Instant.ofEpochSecond(
      filterSpec.collectionInterval.endTime.seconds,
      filterSpec.collectionInterval.endTime.nanos.toLong(),
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
   * @return An EventBatch containing events that match all configured filters. The batch may be
   *   empty if no events match the criteria.
   * @throws Exception if there are issues with event processing, though individual event failures
   *   are logged and do not stop batch processing.
   */
  fun processBatch(batch: EventBatch<T>): EventBatch<T> {
    if (batch.events.isEmpty()) {
      return batch
    }

    if (!filterSpec.eventGroupReferenceIds.contains(batch.eventGroupReferenceId)) {
      return EventBatch(
        emptyList(),
        minTime = batch.minTime,
        maxTime = batch.maxTime,
        eventGroupReferenceId = batch.eventGroupReferenceId,
      )
    }

    // Fast batch-level time range check: skip entire batch if no overlap
    if (!batchTimeRangeOverlaps(batch)) {
      return EventBatch(
        emptyList(),
        minTime = batch.minTime,
        maxTime = batch.maxTime,
        eventGroupReferenceId = batch.eventGroupReferenceId,
      )
    }

    val filteredEvents =
      batch.events.filter { event ->
        if (!isEventInTimeRange(event)) {
          return@filter false
        }

        EventFilters.matches(event.message, program)
      }

    return EventBatch(
      filteredEvents,
      minTime = batch.minTime,
      maxTime = batch.maxTime,
      eventGroupReferenceId = batch.eventGroupReferenceId,
    )
  }

  /**
   * Checks if the batch's time range overlaps with the filter's collection interval.
   *
   * This is a fast batch-level check to avoid processing events when the entire batch is outside
   * the collection interval.
   *
   * @param batch the batch to check
   * @return `true` if the batch time range overlaps with the collection interval, `false` otherwise
   */
  private fun batchTimeRangeOverlaps(batch: EventBatch<T>): Boolean {
    // Check if batch [minTime, maxTime] overlaps with filter [startInstant, endInstant)
    // For the collection interval [start, end), events at exactly start time should be included
    // Overlap exists if: batch.maxTime >= startInstant AND batch.minTime < endInstant
    return !batch.maxTime.isBefore(startInstant) && batch.minTime.isBefore(endInstant)
  }

  /**
   * Checks if an event's timestamp falls within the collection interval.
   *
   * Uses a half-open interval [start, end) where the start time is inclusive and the end time is
   * exclusive. This follows standard time interval conventions.
   *
   * @param event the event to check
   * @return `true` if the event timestamp is within the interval, `false` otherwise
   */
  private fun isEventInTimeRange(event: LabeledEvent<T>): Boolean {
    val eventTime = event.timestamp
    return !eventTime.isBefore(startInstant) && eventTime.isBefore(endInstant)
  }
}
