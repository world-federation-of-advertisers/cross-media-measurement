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
import com.google.protobuf.DynamicMessage
import com.google.protobuf.TypeRegistry
import com.google.type.Interval
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
 * The processor supports filtering by event group reference ID, time ranges, CEL expressions,
 * and VID sampling.
 *
 * @param filterSpec immutable specification containing all filtering criteria
 */
class FilterProcessor(
  val filterSpec: FilterSpec,
) {

  companion object {
    /** Logger instance for FilterProcessor operations. */
    private val logger = Logger.getLogger(FilterProcessor::class.java.name)
  }

  /**
   * Cached event message descriptor from the first processed event.
   */
  private var cachedEventMessageDescriptor: Descriptors.Descriptor? = null

  /**
   * Compiled CEL program for event filtering.
   *
   * Lazily initialized from the first event's message descriptor.
   */
  private var cachedProgram: Program? = null

  /**
   * Cached start time instant from the collection interval.
   *
   * Pre-computed to avoid repeated conversion from protobuf Timestamp to Java Instant
   * during batch processing.
   */
  private val cachedStartInstant: Instant =
    Instant.ofEpochSecond(filterSpec.collectionInterval.startTime.seconds, filterSpec.collectionInterval.startTime.nanos.toLong())

  /**
   * Cached end time instant from the collection interval.
   *
   * Pre-computed to avoid repeated conversion from protobuf Timestamp to Java Instant
   * during batch processing.
   */
  private val cachedEndInstant: Instant =
    Instant.ofEpochSecond(filterSpec.collectionInterval.endTime.seconds, filterSpec.collectionInterval.endTime.nanos.toLong())

  /**
   * Processes a batch of events and returns the matching events.
   *
   * Applies filtering in the following order for optimal performance:
   * 1. Event group reference ID filtering (fastest, string comparison)
   * 2. Time range filtering (fast, cached instant comparisons)
   * 3. CEL expression filtering (slower, requires expression evaluation)
   *
   * @param batch The batch of events to process. Must contain a valid list of events.
   * @return An EventBatch containing events that match all configured filters. The batch may be empty
   *   if no events match the criteria.
   *
   * @throws Exception if there are issues with event processing, though individual
   *   event failures are logged and do not stop batch processing.
   */
  suspend fun processBatch(batch: EventBatch): EventBatch {
    if (batch.events.isEmpty()) {
      return batch
    }

    if (cachedProgram == null) {
      val firstEvent = batch.events.first()
      cachedEventMessageDescriptor = firstEvent.message.descriptorForType
      cachedProgram = if (filterSpec.celExpression.isEmpty()) {
        Program { Program.newEvalResult(BoolT.True, null) }
      } else {
        EventFilters.compileProgram(cachedEventMessageDescriptor!!, filterSpec.celExpression)
      }
    }

    val filteredEvents = batch.events.filter { event ->
      if (event.eventGroupReferenceId != filterSpec.eventGroupReferenceId) {
        return@filter false
      }

      if (!isEventInTimeRange(event)) {
        return@filter false
      }

      if (!isEventInVidSamplingRange(event)) {
        return@filter false
      }

      try {
        EventFilters.matches(event.message, cachedProgram!!)
      } catch (e: Exception) {
        logger.warning("CEL filter evaluation failed for event with VID ${event.vid}: ${e.message}")
        false
      }
    }

    return batch.copy(events = filteredEvents)
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
  private fun isEventInTimeRange(event: LabeledEvent<DynamicMessage>): Boolean {
    try {
      val eventTime = event.timestamp
      return !eventTime.isBefore(cachedStartInstant) && eventTime.isBefore(cachedEndInstant)
    } catch (e: Exception) {
      logger.warning("Time range evaluation failed for event with VID ${event.vid}: ${e.message}")
      return false
    }
  }

  /**
   * Checks if an event's VID falls within the configured sampling range.
   *
   * Uses the normalized VID value (VID / population size) to determine if the event
   * should be included based on the sampling start position and width.
   *
   * @param event the event to check
   * @return `true` if the event VID is within the sampling range, `false` otherwise
   */
  private fun isEventInVidSamplingRange(event: LabeledEvent<DynamicMessage>): Boolean {
    val samplingStart = filterSpec.vidSamplingStart
    val samplingEnd = samplingStart + filterSpec.vidSamplingWidth

    return event.vid in samplingStart..<samplingEnd
  }
}
