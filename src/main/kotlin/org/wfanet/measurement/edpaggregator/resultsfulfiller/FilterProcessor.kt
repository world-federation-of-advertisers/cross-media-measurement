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
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters

/**
 * Filter processor for event filtering with CEL expressions and time ranges.
 *
 * This processor compiles a CEL expression once and reuses it for batch processing. It processes
 * events sequentially through the CEL filter and identifies matching events.
 *
 * Batch-level selector dispatch (reference-id vs entity-key) is handled by [FilterSpec.matchBatch],
 * which returns a [BatchMatchResult].
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
   * 1. Batch-level selector check via [FilterSpec.matchBatch].
   * 2. Batch time range overlap check (fast, avoids processing entire batch).
   * 3. Per-event time range filter.
   * 4. Per-event entity-key intersection (only for [FilterSpec.ByEntityKeys]).
   * 5. Per-event CEL expression evaluation.
   *
   * @param batch The batch of events to process. Must contain a valid list of events.
   * @return An EventBatch containing events that match all configured filters. The batch may be
   *   empty if no events match the criteria.
   * @throws MissingBatchEntityKeysException when [filterSpec] is [FilterSpec.ByEntityKeys] and
   *   [batch.eventGroupIdentifier] is [EventGroupIdentifier.ByEntityKeys] with an empty entity keys
   *   list.
   */
  fun processBatch(batch: EventBatch<T>): EventBatch<T> {
    if (batch.events.isEmpty()) {
      return batch
    }

    val matchResult = filterSpec.matchBatch(batch.eventGroupIdentifier)
    if (matchResult is BatchMatchResult.NoMatch) {
      return emptyBatchLike(batch)
    }

    // Fast batch-level time range check: skip entire batch if no overlap
    if (!batchTimeRangeOverlaps(batch)) {
      return emptyBatchLike(batch)
    }

    val filteredEvents =
      batch.events.filter { event ->
        if (!isEventInTimeRange(event)) {
          return@filter false
        }

        if (
          matchResult is BatchMatchResult.MatchedByEntityKeys &&
            !eventMatchesEntityKeyFilter(event, matchResult.entityKeyFilter)
        ) {
          return@filter false
        }

        EventFilters.matches(event.message, program)
      }

    return EventBatch(
      filteredEvents,
      minTime = batch.minTime,
      maxTime = batch.maxTime,
      eventGroupIdentifier = batch.eventGroupIdentifier,
    )
  }

  /**
   * Returns an empty `EventBatch` carrying the same metadata as [batch].
   *
   * Used to short-circuit a batch without dropping its identifying metadata so downstream
   * accounting can still distinguish per-batch outputs.
   */
  private fun emptyBatchLike(batch: EventBatch<T>): EventBatch<T> {
    return EventBatch(
      emptyList(),
      minTime = batch.minTime,
      maxTime = batch.maxTime,
      eventGroupIdentifier = batch.eventGroupIdentifier,
    )
  }

  /**
   * Checks if the batch's time range overlaps with the filter's collection interval.
   *
   * This is a fast batch-level check to avoid processing events when the entire batch is outside
   * the collection interval.
   */
  private fun batchTimeRangeOverlaps(batch: EventBatch<T>): Boolean {
    return !batch.maxTime.isBefore(startInstant) && batch.minTime.isBefore(endInstant)
  }

  /**
   * Checks if an event's timestamp falls within the collection interval.
   *
   * Uses a half-open interval [start, end) where the start time is inclusive and the end time is
   * exclusive.
   */
  private fun isEventInTimeRange(event: LabeledEvent<T>): Boolean {
    val eventTime = event.timestamp
    return !eventTime.isBefore(startInstant) && eventTime.isBefore(endInstant)
  }

  /**
   * Checks whether [event]'s `entityKeys` intersect [filter].
   *
   * An event with empty `entityKeys` always fails this check (cannot match a non-empty filter).
   */
  private fun eventMatchesEntityKeyFilter(
    event: LabeledEvent<T>,
    filter: Set<LabeledImpression.EntityKey>,
  ): Boolean {
    return event.entityKeys.any { it in filter }
  }
}
