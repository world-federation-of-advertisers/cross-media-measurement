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
 * Thrown by [FilterProcessor.processBatch] when the active selector is [FilterSpec.ByEntityKeys]
 * but the incoming [EventBatch] carries no `entity_keys`.
 *
 * Indicates a wiring/data invariant violation upstream (the blob's `BlobDetails.entity_keys` was
 * not populated despite the consumer requesting entity-key filtering).
 */
class MissingBatchEntityKeysException :
  IllegalStateException(
    "ByEntityKeys filter requires the batch to carry entity_keys, but batch.entityKeys is empty"
  )

/**
 * Filter processor for event filtering with CEL expressions and time ranges.
 *
 * This processor compiles a CEL expression once and reuses it for batch processing. It processes
 * events sequentially through the CEL filter and identifies matching events.
 *
 * The processor dispatches on [FilterSpec]'s variant to choose the EventGroup selector:
 * - [FilterSpec.ByEventGroupReferenceIds] uses the legacy `eventGroupReferenceId` batch-level
 *   match.
 * - [FilterSpec.ByEntityKeys] uses a batch-level entity-key intersection short-circuit followed by
 *   a per-event entity-key check.
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
   * 1. Selector check, dispatched on [FilterSpec] variant:
   *     - [FilterSpec.ByEventGroupReferenceIds]: batch-level `event_group_reference_id` match.
   *     - [FilterSpec.ByEntityKeys]: batch-level entity-key intersection short-circuit. Throws
   *       [MissingBatchEntityKeysException] when the batch carries no entity_keys (data invariant
   *       violation).
   * 2. Batch time range overlap check (fast, avoids processing entire batch).
   * 3. Per-event time range filter.
   * 4. Per-event entity-key intersection (only for [FilterSpec.ByEntityKeys]).
   * 5. Per-event CEL expression evaluation.
   *
   * @param batch The batch of events to process. Must contain a valid list of events.
   * @return An EventBatch containing events that match all configured filters. The batch may be
   *   empty if no events match the criteria.
   * @throws MissingBatchEntityKeysException when [filterSpec] is [FilterSpec.ByEntityKeys] and
   *   [batch.entityKeys] is empty.
   */
  fun processBatch(batch: EventBatch<T>): EventBatch<T> {
    if (batch.events.isEmpty()) {
      return batch
    }

    val entityKeyFilter: Set<LabeledImpression.EntityKey>? =
      when (val spec = filterSpec) {
        is FilterSpec.ByEventGroupReferenceIds -> {
          val identifier =
            batch.eventGroupIdentifier as? EventGroupIdentifier.ByReferenceId
              ?: return emptyBatchLike(batch)
          if (!spec.eventGroupReferenceIds.contains(identifier.refId)) {
            return emptyBatchLike(batch)
          }
          null
        }
        is FilterSpec.ByEntityKeys -> {
          val identifier =
            batch.eventGroupIdentifier as? EventGroupIdentifier.ByEntityKeys
              ?: throw MissingBatchEntityKeysException()
          if (identifier.entityKeys.isEmpty()) throw MissingBatchEntityKeysException()
          if (!batchEntityKeysOverlap(identifier.entityKeys, spec.entityKeys)) {
            return emptyBatchLike(batch)
          }
          spec.entityKeys
        }
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

        if (entityKeyFilter != null && !eventMatchesEntityKeyFilter(event, entityKeyFilter)) {
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

  /**
   * Checks whether any [org.wfanet.measurement.edpaggregator.v1alpha.EntityKeyGroup] on [batch]
   * contains an `(entity_type, entity_id)` pair that matches [filter].
   *
   * Pre-flattens the batch's grouped entity keys into a `Set<EntityKeyPair>` once, then performs
   * O(1) membership lookups for each filter element. Faster than nested `any { contains(...) }` for
   * batches with many entity keys.
   *
   * Caller must ensure `batch.entityKeys` is non-empty before invoking.
   */
  private fun batchEntityKeysOverlap(
    batchEntityKeys: List<org.wfanet.measurement.edpaggregator.v1alpha.EntityKeyGroup>,
    filter: Set<LabeledImpression.EntityKey>,
  ): Boolean {
    val batchKeyPairs: Set<EntityKeyPair> =
      batchEntityKeys
        .flatMap { g -> g.entityIdsList.map { id -> EntityKeyPair(g.entityType, id) } }
        .toSet()
    return filter.any { fk -> EntityKeyPair(fk.entityType, fk.entityId) in batchKeyPairs }
  }

  /** Flattened (entity_type, entity_id) pair for O(1) batch entity-key lookups. */
  private data class EntityKeyPair(val entityType: String, val entityId: String)
}
