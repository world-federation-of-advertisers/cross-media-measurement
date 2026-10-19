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

/**
 * Thrown when a [FilterSpec] is constructed with a `collectionInterval` whose `startTime` is not
 * strictly before its `endTime`.
 */
class InvalidCollectionIntervalException :
  IllegalArgumentException("collectionInterval startTime must be before endTime")

/** Thrown by [FilterSpec.matchBatch] when the batch identifier does not contain any selector. */
class EmptyEventGroupIdentifierException :
  IllegalStateException("EventGroupIdentifier does not contain any selector")

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
   * Returned when matching a whole-blob selector, or when the batch's entity keys are fully
   * contained within the filter's entity keys.
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
 * Immutable event filter and deduplication key.
 *
 * [eventGroupReferenceIds] and [entityKeys] contain both native requisition selectors and any
 * configured equivalents.
 */
data class FilterSpec(
  val celExpression: String,
  val collectionInterval: Interval,
  val eventGroupReferenceIds: Set<String>,
  val entityKeys: Set<LabeledImpression.EntityKey>,
) {
  init {
    require(eventGroupReferenceIds.isNotEmpty() || entityKeys.isNotEmpty()) {
      "At least one event-group selector must be present"
    }
    if (
      !collectionInterval.startTime.toInstant().isBefore(collectionInterval.endTime.toInstant())
    ) {
      throw InvalidCollectionIntervalException()
    }
  }

  private val entityKeyPairs: Set<Pair<String, String>> =
    entityKeys.map { Pair(it.entityType, it.entityId) }.toSet()

  /**
   * Checks whether [identifier] matches this filter's batch-level selector.
   *
   * @param identifier the representation of the event group in the batch's blob metadata.
   * @return [BatchMatchResult.NoMatch] if the batch should be skipped,
   *   [BatchMatchResult.MatchedAllEvents] or [BatchMatchResult.MatchedByEntityKeys] if it passed.
   * @throws EmptyEventGroupIdentifierException when [identifier] contains no selector.
   */
  fun matchBatch(identifier: EventGroupIdentifier): BatchMatchResult {
    if (
      identifier.eventGroupReferenceId != null &&
        identifier.eventGroupReferenceId in eventGroupReferenceIds
    ) {
      return BatchMatchResult.MatchedAllEvents
    }

    val wholeBlobEntityKey: LabeledImpression.EntityKey? = identifier.wholeBlobEntityKey
    if (
      wholeBlobEntityKey != null &&
        Pair(wholeBlobEntityKey.entityType, wholeBlobEntityKey.entityId) in entityKeyPairs
    ) {
      return BatchMatchResult.MatchedAllEvents
    }

    val identifierIsEmpty: Boolean =
      identifier.eventGroupReferenceId == null &&
        identifier.entityKeys.isEmpty() &&
        identifier.wholeBlobEntityKey == null
    if (identifierIsEmpty) {
      throw EmptyEventGroupIdentifierException()
    }

    if (identifier.entityKeys.isEmpty() || entityKeys.isEmpty()) {
      return BatchMatchResult.NoMatch
    }
    val batchKeyPairs: Set<Pair<String, String>> =
      identifier.entityKeys.map { Pair(it.entityType, it.entityId) }.toSet()
    if (entityKeyPairs.none { it in batchKeyPairs }) {
      return BatchMatchResult.NoMatch
    }
    return if (batchKeyPairs.all { it in entityKeyPairs }) {
      BatchMatchResult.MatchedAllEvents
    } else {
      BatchMatchResult.MatchedByEntityKeys(entityKeyFilter = entityKeys)
    }
  }
}
