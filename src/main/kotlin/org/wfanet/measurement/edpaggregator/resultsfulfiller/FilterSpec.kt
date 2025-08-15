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

/**
 * Immutable specification for event filtering.
 *
 * This data class serves as a unique key for deduplicating event groups across requisitions
 * and for looking up frequency vector sinks in the pipeline.
 *
 * @property celExpression The CEL expression for filtering events
 * @property collectionInterval The time interval for event collection
 * @property vidSamplingStart The starting VID for sampling (inclusive)
 * @property vidSamplingWidth The width of the VID sampling interval
 * @property eventGroupReferenceId The reference ID for the event group
 */
data class FilterSpec(
  val celExpression: String,
  val collectionInterval: Interval,
  val vidSamplingStart: Float,
  val vidSamplingWidth: Float,
  val eventGroupReferenceId: String
) {
  init {
    require(eventGroupReferenceId.isNotBlank()) { "Event group reference ID must not be blank" }
    require(vidSamplingWidth > 0) { "VID sampling width must be positive" }
  }
}
