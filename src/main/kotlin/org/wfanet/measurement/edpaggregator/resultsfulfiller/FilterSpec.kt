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
import org.wfanet.measurement.edpaggregator.v1alpha.EntityKeyGroup
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpressionKt.entityKey

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
   * @param mapping optional equivalence between reference-ID and entity-key representations.
   * @return [BatchMatchResult.NoMatch] if the batch should be skipped,
   *   [BatchMatchResult.MatchedAllEvents] or [BatchMatchResult.MatchedByEntityKeys] if it passed.
   * @throws MismatchedBatchIdentifierException when [identifier]'s variant does not match this
   *   [FilterSpec] variant and [mapping] is `null`.
   * @throws MissingBatchEntityKeysException when [identifier] carries an empty entity-keys list.
   */
  abstract fun matchBatch(
    identifier: EventGroupIdentifier,
    mapping: EventGroupReferenceIdEntityKeyMapping?,
  ): BatchMatchResult

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
   */
  data class ByEventGroupReferenceIds(
    override val celExpression: String,
    override val collectionInterval: Interval,
    val eventGroupReferenceIds: List<String>,
  ) : FilterSpec() {
    init {
      if (eventGroupReferenceIds.isEmpty()) throw EmptyEventGroupReferenceIdsException()
      requireValidCollectionInterval(collectionInterval)
    }

    override fun matchBatch(
      identifier: EventGroupIdentifier,
      mapping: EventGroupReferenceIdEntityKeyMapping?,
    ): BatchMatchResult {
      return when (identifier) {
        is EventGroupIdentifier.ByReferenceId -> matchReferenceId(identifier.refId)
        is EventGroupIdentifier.ByEntityKeys ->
          matchEntityKeys(
            identifier.entityKeys,
            mapping
              ?: throw MismatchedBatchIdentifierException(
                "ByEventGroupReferenceIds filter requires EventGroupIdentifier.ByReferenceId"
              ),
          )
      }
    }

    private fun matchReferenceId(refId: String): BatchMatchResult {
      return if (refId in eventGroupReferenceIds) {
        BatchMatchResult.MatchedAllEvents
      } else {
        BatchMatchResult.NoMatch
      }
    }

    private fun matchEntityKeys(
      batchEntityKeys: List<EntityKeyGroup>,
      mapping: EventGroupReferenceIdEntityKeyMapping,
    ): BatchMatchResult {
      if (batchEntityKeys.isEmpty()) throw MissingBatchEntityKeysException()
      val batchEntityIds: Set<String> =
        batchEntityKeys
          .asSequence()
          .filter { it.entityType == mapping.entityType }
          .flatMap { it.entityIdsList.asSequence() }
          .toSet()
      val requestedEntityIds: Set<String> =
        eventGroupReferenceIds.mapNotNull(mapping::toEntityId).toSet()

      return when {
        batchEntityIds.none { it in requestedEntityIds } -> BatchMatchResult.NoMatch
        batchEntityIds.size == 1 -> BatchMatchResult.MatchedAllEvents
        else ->
          BatchMatchResult.MatchedByEntityKeys(
            requestedEntityIds
              .map { entityId ->
                entityKey {
                  entityType = mapping.entityType
                  this.entityId = entityId
                }
              }
              .toSet()
          )
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
   */
  data class ByEntityKeys(
    override val celExpression: String,
    override val collectionInterval: Interval,
    val entityKeys: Set<LabeledImpression.EntityKey>,
  ) : FilterSpec() {
    init {
      if (entityKeys.isEmpty()) throw EmptyEntityKeysException()
      requireValidCollectionInterval(collectionInterval)
    }

    private val filterKeyPairs: Set<Pair<String, String>> =
      entityKeys.map { Pair(it.entityType, it.entityId) }.toSet()

    override fun matchBatch(
      identifier: EventGroupIdentifier,
      mapping: EventGroupReferenceIdEntityKeyMapping?,
    ): BatchMatchResult {
      return when (identifier) {
        is EventGroupIdentifier.ByReferenceId ->
          matchMappedReferenceId(
            identifier.refId,
            mapping
              ?: throw MismatchedBatchIdentifierException(
                "ByEntityKeys filter requires EventGroupIdentifier.ByEntityKeys"
              ),
          )
        is EventGroupIdentifier.ByEntityKeys -> matchEntityKeys(identifier.entityKeys, mapping)
      }
    }

    private fun matchMappedReferenceId(
      refId: String,
      mapping: EventGroupReferenceIdEntityKeyMapping,
    ): BatchMatchResult {
      if (refId.isEmpty()) {
        return BatchMatchResult.NoMatch
      }
      val entityId = mapping.toEntityId(refId) ?: return BatchMatchResult.NoMatch
      return if (Pair(mapping.entityType, entityId) in filterKeyPairs) {
        BatchMatchResult.MatchedAllEvents
      } else {
        BatchMatchResult.NoMatch
      }
    }

    private fun matchEntityKeys(
      batchEntityKeys: List<EntityKeyGroup>,
      mapping: EventGroupReferenceIdEntityKeyMapping?,
    ): BatchMatchResult {
      if (batchEntityKeys.isEmpty()) throw MissingBatchEntityKeysException()
      val batchKeyPairs: Set<Pair<String, String>> =
        batchEntityKeys
          .flatMap { group ->
            group.entityIdsList.map { entityId -> Pair(group.entityType, entityId) }
          }
          .toSet()
      if (filterKeyPairs.none { it in batchKeyPairs }) {
        return BatchMatchResult.NoMatch
      }
      if (mapping != null) {
        val mappedBatchIds: Set<String> =
          batchKeyPairs.filter { it.first == mapping.entityType }.map { it.second }.toSet()
        val mappedFilterIds: Set<String> =
          filterKeyPairs.filter { it.first == mapping.entityType }.map { it.second }.toSet()
        if (mappedBatchIds.size == 1 && mappedBatchIds.single() in mappedFilterIds) {
          return BatchMatchResult.MatchedAllEvents
        }
      }
      return if (batchKeyPairs.all { it in filterKeyPairs }) {
        BatchMatchResult.MatchedAllEvents
      } else {
        BatchMatchResult.MatchedByEntityKeys(entityKeyFilter = entityKeys)
      }
    }
  }
}
