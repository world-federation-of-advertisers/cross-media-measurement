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

/**
 * Immutable specification for event filtering.
 *
 * This data class serves two purposes:
 * - as a unique key for looking up / deduplicating frequency vector sinks in the pipeline.
 * - as a parameter for the actual filtering.
 *
 * @property celExpression The CEL expression for filtering events
 * @property collectionInterval The time interval for event collection
 * @property eventGroupReferenceIds The reference IDs of the event groups to be filtered
 */
data class FilterSpec(
  val celExpression: String,
  val collectionInterval: Interval,
  val eventGroupReferenceIds: List<String>,
) {
  init {
    require(eventGroupReferenceIds.isNotEmpty()) { "eventGroupReferenceIds must not be empty" }
    require(
      collectionInterval.startTime.toInstant().isBefore(collectionInterval.endTime.toInstant())
    ) {
      "collectionInterval startTime must be before endTime"
    }
  }
}
