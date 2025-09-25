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

import com.google.protobuf.Message

/**
 * Simple event batch container for efficient processing.
 *
 * Groups events together for batch processing through the pipeline, reducing overhead and improving
 * throughput.
 *
 * @param events A batch of parsed events
 * @param minTime The earliest event time in the batch, used for fast filtering.
 * @param maxTime The latest event time in the batch, used for filtering.
 * @param eventGroupReferenceId identifier linking this event to a specific event group or campaign.
 *   Used for filtering events by group membership.
 */
data class EventBatch<T : Message>(
  val events: List<LabeledEvent<T>>,
  val minTime: java.time.Instant,
  val maxTime: java.time.Instant,
  val eventGroupReferenceId: String,
) {
  val size: Int
    get() = events.size
}
