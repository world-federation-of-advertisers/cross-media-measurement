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
 * Interface for event processing pipelines.
 *
 * Different implementations can provide different strategies for processing event batches, such as
 * sequential processing, parallel processing, or distributed processing.
 */
interface EventProcessingPipeline<T : Message> {

  /**
   * Processes event batches from the given [eventSource] through the filters of the given [sinks].
   * For each event that matches a sink’s filter, the sink increments its frequency vector. One
   * event can match multiple sinks.
   *
   * @param eventSource Source that provides event batches to process
   * @param sinks FrequencyVector sinks that count the result
   */
  suspend fun processEventBatches(eventSource: EventSource<T>, sinks: List<FrequencyVectorSink<T>>)
}
