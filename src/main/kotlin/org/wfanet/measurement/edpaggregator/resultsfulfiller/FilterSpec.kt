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

import com.google.type.Interval
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression

/** Thrown when a [FilterSpec] is constructed with an empty `eventGroupReferenceIds` selector. */
class EmptyEventGroupReferenceIdsException :
  IllegalArgumentException("eventGroupReferenceIds must not be empty")

/** Thrown when a [FilterSpec] is constructed with an empty `entityKeys` selector. */
class EmptyEntityKeysException : IllegalArgumentException("entityKeys must not be empty")

/**
 * Thrown when a [FilterSpec] is constructed with a `collectionInterval` whose `startTime` is not
 * strictly before its `endTime`.
 */
class InvalidCollectionIntervalException :
  IllegalArgumentException("collectionInterval startTime must be before endTime")

/**
 * Thrown by [FilterSpec.matchBatch] when the batch identifier carries an empty entity-keys list.
 */
class MissingBatchEntityKeysException :
  IllegalStateException("Batch EventGroupIdentifier.ByEntityKeys has an empty entity keys list")

/**
 * Thrown by [FilterSpec.matchBatch] when the [EventGroupIdentifier] variant does not match the
 * [FilterSpec] variant.
 */
class MismatchedBatchIdentifierException(message: String) : IllegalStateException(message)

/**
 * Result of [FilterSpec.matchBatch].
 *
 * @see FilterSpec.matchBatch
 */
sealed class BatchMatchResult {
  /** The batch does not match this filter. */
  object NoMatch : BatchMatchResult()

  /**
   * All events in the batch match the filter; no per-event entity-key filtering needed.
   *
   * Returned when matching by reference ID, or when the batch's entity keys are fully contained
   * within the filter's entity keys.
   */
  object MatchedAllEvents : BatchMatchResult()

  /**
   * The batch partially overlaps the filter's entity keys; per-event filtering is required.
   *
   * @property entityKeyFilter The set of entity keys to filter individual events against.
   */
  data class MatchedByEntityKeys(val entityKeyFilter: Set<LabeledImpression.EntityKey>) :
    BatchMatchResult()
}

/**
 * Immutable specification for event filtering.
 *
 * This sealed type serves two purposes:
 * - as a unique key for looking up / deduplicating frequency vector sinks in the pipeline.
 * - as a parameter for the actual filtering.
 *
 * The two variants represent the two alternative selectors for identifying which events belong to
 * an EventGroup: legacy reference IDs ([ByEventGroupReferenceIds]) and entity keys
 * ([ByEntityKeys]). Selecting a variant at the type level guarantees that exactly one selector is
 * populated and that empty placeholder values cannot be passed by callers.
 */
sealed class FilterSpec {
  /** The CEL expression for filtering events. */
  abstract val celExpression: String

  /** The time interval for event collection. */
  abstract val collectionInterval: Interval

  /**
   * Checks whether [identifier] matches this filter's batch-level selector.
   *
   * @param identifier the representation of the event group in the batch's blob metadata.
   * @return [BatchMatchResult.NoMatch] if the batch should be skipped,
   *   [BatchMatchResult.MatchedAllEvents] or [BatchMatchResult.MatchedByEntityKeys] if it passed.
   * @throws MismatchedBatchIdentifierException when [identifier]'s variant does not match this
   *   [FilterSpec] variant and no equivalent selector is available.
   * @throws MissingBatchEntityKeysException when [identifier] carries an empty entity-keys list.
   */
  abstract fun matchBatch(identifier: EventGroupIdentifier): BatchMatchResult

  protected fun requireValidCollectionInterval(interval: Interval) {
    if (!interval.startTime.toInstant().isBefore(interval.endTime.toInstant())) {
      throw InvalidCollectionIntervalException()
    }
  }

  /**
   * Legacy selector: filter events by their batch's `eventGroupReferenceId`.
   *
   * @property eventGroupReferenceIds The reference IDs of the event groups to be filtered. Must be
   *   non-empty.
   * @property mappedEntityKeys Entity keys equivalent to [eventGroupReferenceIds].
   */
  data class ByEventGroupReferenceIds(
    override val celExpression: String,
    override val collectionInterval: Interval,
    val eventGroupReferenceIds: List<String>,
    val mappedEntityKeys: Set<LabeledImpression.EntityKey>,
  ) : FilterSpec() {
    init {
      if (eventGroupReferenceIds.isEmpty()) throw EmptyEventGroupReferenceIdsException()
      requireValidCollectionInterval(collectionInterval)
    }

    private val mappedEntityKeyPairs: Set<Pair<String, String>> =
      mappedEntityKeys.map { Pair(it.entityType, it.entityId) }.toSet()
    private val mappedEntityTypes: Set<String> = mappedEntityKeys.map { it.entityType }.toSet()

    override fun matchBatch(identifier: EventGroupIdentifier): BatchMatchResult {
      return when (identifier) {
        is EventGroupIdentifier.ByReferenceId -> matchReferenceId(identifier.refId)
        is EventGroupIdentifier.ByEntityKeys -> matchEntityKeys(identifier)
      }
    }

    private fun matchReferenceId(refId: String): BatchMatchResult {
      return if (refId in eventGroupReferenceIds) {
        BatchMatchResult.MatchedAllEvents
      } else {
        BatchMatchResult.NoMatch
      }
    }

    private fun matchEntityKeys(identifier: EventGroupIdentifier.ByEntityKeys): BatchMatchResult {
      if (identifier.refId != null && identifier.refId in eventGroupReferenceIds) {
        return BatchMatchResult.MatchedAllEvents
      }
      if (identifier.entityKeys.isEmpty()) throw MissingBatchEntityKeysException()
      if (mappedEntityKeys.isEmpty()) {
        throw MismatchedBatchIdentifierException(
          "ByEventGroupReferenceIds filter requires EventGroupIdentifier.ByReferenceId"
        )
      }
      val batchKeyPairs: Set<Pair<String, String>> =
        identifier.entityKeys
          .asSequence()
          .flatMap { group ->
            group.entityIdsList.asSequence().map { entityId -> Pair(group.entityType, entityId) }
          }
          .toSet()
      if (mappedEntityKeyPairs.none { it in batchKeyPairs }) {
        return BatchMatchResult.NoMatch
      }
      val batchMappedKeyPairs: Set<Pair<String, String>> =
        batchKeyPairs.filter { it.first in mappedEntityTypes }.toSet()

      return if (batchMappedKeyPairs.size == 1) {
        BatchMatchResult.MatchedAllEvents
      } else {
        BatchMatchResult.MatchedByEntityKeys(mappedEntityKeys)
      }
    }
  }

  /**
   * Entity-key selector: filter events by intersecting the blob's and per-impression entity keys
   * against this set.
   *
   * @property entityKeys Entity keys identifying the EventGroup(s) to be filtered. An event passes
   *   when its `LabeledEvent.entityKeys` intersects this set (OR-semantics across the set). Must be
   *   non-empty.
   * @property mappedEventGroupReferenceIds Event-group reference IDs equivalent to [entityKeys].
   */
  data class ByEntityKeys(
    override val celExpression: String,
    override val collectionInterval: Interval,
    val entityKeys: Set<LabeledImpression.EntityKey>,
    val mappedEventGroupReferenceIds: Set<String>,
  ) : FilterSpec() {
    init {
      if (entityKeys.isEmpty()) throw EmptyEntityKeysException()
      requireValidCollectionInterval(collectionInterval)
    }

    private val filterKeyPairs: Set<Pair<String, String>> =
      entityKeys.map { Pair(it.entityType, it.entityId) }.toSet()

    override fun matchBatch(identifier: EventGroupIdentifier): BatchMatchResult {
      return when (identifier) {
        is EventGroupIdentifier.ByReferenceId -> matchReferenceId(identifier)
        is EventGroupIdentifier.ByEntityKeys -> matchEntityKeys(identifier)
      }
    }

    private fun matchReferenceId(identifier: EventGroupIdentifier.ByReferenceId): BatchMatchResult {
      if (identifier.entityKey == null && mappedEventGroupReferenceIds.isEmpty()) {
        throw MismatchedBatchIdentifierException(
          "ByEntityKeys filter requires EventGroupIdentifier.ByEntityKeys"
        )
      }
      return if (
        identifier.refId in mappedEventGroupReferenceIds || identifier.entityKey in entityKeys
      ) {
        BatchMatchResult.MatchedAllEvents
      } else {
        BatchMatchResult.NoMatch
      }
    }

    private fun matchEntityKeys(identifier: EventGroupIdentifier.ByEntityKeys): BatchMatchResult {
      if (identifier.entityKeys.isEmpty()) throw MissingBatchEntityKeysException()
      val batchKeyPairs: Set<Pair<String, String>> =
        identifier.entityKeys
          .flatMap { group ->
            group.entityIdsList.map { entityId -> Pair(group.entityType, entityId) }
          }
          .toSet()
      if (filterKeyPairs.none { it in batchKeyPairs }) {
        return BatchMatchResult.NoMatch
      }
      if (identifier.refId != null && identifier.refId in mappedEventGroupReferenceIds) {
        return BatchMatchResult.MatchedAllEvents
      }
      return if (batchKeyPairs.all { it in filterKeyPairs }) {
        BatchMatchResult.MatchedAllEvents
      } else {
        BatchMatchResult.MatchedByEntityKeys(entityKeyFilter = entityKeys)
      }
    }
  }
}
