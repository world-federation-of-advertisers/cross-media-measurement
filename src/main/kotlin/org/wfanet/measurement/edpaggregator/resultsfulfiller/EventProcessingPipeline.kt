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
 * Different implementations can provide various processing strategies
 * (e.g., single-threaded, parallel, distributed).
 */
interface EventProcessingPipeline {
  
  /**
   * Processes a flow of event batches through the pipeline.
   * 
   * @param eventBatchFlow The flow of event batches to process
   * @param vidIndexMap Mapping from VIDs to array indices
   * @param filters The filters to apply with their configurations
   * @return Statistics for each filter after processing
   */
  suspend fun processEventBatches(
    eventBatchFlow: Flow<List<LabeledEvent<DynamicMessage>>>,
    vidIndexMap: VidIndexMap,
    filters: List<FilterConfiguration>,
    typeRegistry: TypeRegistry
  ): Map<String, SinkStatistics>
  
  /**
   * Returns the pipeline type for logging/monitoring.
   */
  val pipelineType: String
}