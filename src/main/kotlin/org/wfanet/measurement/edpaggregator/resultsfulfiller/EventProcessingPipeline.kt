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

import com.google.protobuf.DynamicMessage
import com.google.protobuf.TypeRegistry
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.eventdataprovider.shareshuffle.v2alpha.VidIndexMap

/**
 * Interface for event processing pipelines.
 * 
 * Different implementations can provide different strategies for processing
 * event batches, such as sequential processing, parallel processing, or 
 * distributed processing.
 */
interface EventProcessingPipeline {
  
  /**
   * The type identifier for this pipeline implementation.
   */
  val pipelineType: String
  
  /**
   * Process event batches from an event source through the configured filters.
   * 
   * @param eventSource Source that provides event batches to process
   * @param vidIndexMap Mapping from VIDs to frequency vector indices
   * @param filters List of filter configurations to apply
   * @param typeRegistry Registry for protobuf message types
   * @return Map from filter specifications to their resulting frequency vectors
   */
  suspend fun processEventBatches(
    eventSource: EventSource,
    vidIndexMap: VidIndexMap,
    filters: List<FilterConfiguration>,
    typeRegistry: TypeRegistry
  ): Map<FilterSpec, FrequencyVector>
}