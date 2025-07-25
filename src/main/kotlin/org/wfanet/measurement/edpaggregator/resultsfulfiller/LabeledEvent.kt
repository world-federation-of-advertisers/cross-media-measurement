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
 * Represents an event with associated labels for processing.
 * 
 * This is a foundational data structure that wraps events with their metadata
 * for use throughout the processing pipeline.
 * 
 * @param T The protobuf message type of the event
 * @param event The actual event data
 * @param vid The virtual ID associated with this event
 * @param labels Additional labels associated with the event
 */
data class LabeledEvent<T : Message>(
  val event: T,
  val vid: Long,
  val labels: Map<String, String> = emptyMap()
)