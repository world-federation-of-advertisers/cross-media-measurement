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
   * Legacy selector: filter events by their batch's `eventGroupReferenceId`.
   *
   * @property eventGroupReferenceIds The reference IDs of the event groups to be filtered. Must
   *   be non-empty.
   */
  data class ByEventGroupReferenceIds(
    override val celExpression: String,
    override val collectionInterval: Interval,
    val eventGroupReferenceIds: List<String>,
  ) : FilterSpec() {
    init {
      require(eventGroupReferenceIds.isNotEmpty()) { "eventGroupReferenceIds must not be empty" }
      requireValidCollectionInterval(collectionInterval)
    }
  }

  /**
   * Entity-key selector: filter events by intersecting the blob's and per-impression entity keys
   * against this set.
   *
   * @property entityKeys Entity keys identifying the EventGroup(s) to be filtered. An event passes
   *   when its `LabeledEvent.entityKeys` intersects this set (OR-semantics across the set). Must
   *   be non-empty.
   */
  data class ByEntityKeys(
    override val celExpression: String,
    override val collectionInterval: Interval,
    val entityKeys: Set<LabeledImpression.EntityKey>,
  ) : FilterSpec() {
    init {
      require(entityKeys.isNotEmpty()) { "entityKeys must not be empty" }
      requireValidCollectionInterval(collectionInterval)
    }
  }

  protected fun requireValidCollectionInterval(interval: Interval) {
    require(interval.startTime.toInstant().isBefore(interval.endTime.toInstant())) {
      "collectionInterval startTime must be before endTime"
    }
  }
}
