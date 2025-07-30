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
import java.time.LocalDate
import java.time.ZoneId
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec

/**
 * Configuration for the event processing pipeline.
 *
 * This data class encapsulates all configuration parameters needed to run
 * the pipeline, making it easier to pass configuration between components.
 */
data class PipelineConfiguration(
  // Date range configuration
  val startDate: LocalDate,
  val endDate: LocalDate,

  // Pipeline processing configuration
  val batchSize: Int,
  val channelCapacity: Int,
  val useParallelPipeline: Boolean,
  val parallelBatchSize: Int,
  val parallelWorkers: Int,
  val threadPoolSize: Int,

  // Event source configuration
  val eventSourceType: EventSourceType,

  // Synthetic data generation configuration (required if eventSourceType is SYNTHETIC)
  val populationSpec: SyntheticPopulationSpec? = null,
  val eventGroupSpec: SyntheticEventGroupSpec? = null,

  // Storage event source configuration (required if eventSourceType is STORAGE)
  val eventReader: EventReader? = null,
  val eventGroupReferenceIds: List<String> = emptyList(),
  
  // Population spec file path (optional - if not provided, population spec will be generated)
  // TODO: Switch to fetch population spec from the API instead of loading from file
  val populationSpecPath: String? = null,

  val zoneId: ZoneId,

  // Optional time filtering
  val collectionInterval: Interval? = null,

  // Logging configuration
  val disableLogging: Boolean = false,
  
  // If true, the population spec file is treated as a SyntheticPopulationSpec and converted
  val isSyntheticPopulationSpec: Boolean = false,
  
  // Maximum frequency cap for measurements (0 means no capping, uses technical byte limit)
  val maxFrequency: Int = 0
) {

  /**
   * Validates the configuration parameters.
   * @throws IllegalArgumentException if any parameter is invalid
   */
  fun validate() {
    require(endDate >= startDate) { "End date must be after or equal to start date" }
    require(batchSize > 0) { "Batch size must be positive" }
    require(channelCapacity > 0) { "Channel capacity must be positive" }
    require(parallelBatchSize > 0) { "Parallel batch size must be positive" }
    require(parallelWorkers > 0) { "Parallel workers must be positive" }
    require(threadPoolSize > 0) { "Thread pool size must be positive" }

    when (eventSourceType) {
      EventSourceType.SYNTHETIC -> {
        requireNotNull(populationSpec) { "Population spec is required for synthetic event source" }
        requireNotNull(eventGroupSpec) { "Event group spec is required for synthetic event source" }
        require(populationSpec.hasVidRange()) { "Population spec must have VID range" }
        require(eventGroupSpec.dateSpecsCount > 0) { "Event group spec must have date specifications" }
      }
      EventSourceType.STORAGE -> {
        requireNotNull(eventReader) { "Event reader is required for storage event source" }
        require(eventGroupReferenceIds.isNotEmpty()) { "Event group reference IDs must not be empty for storage event source" }
      }
    }
  }

  /**
   * Returns the effective batch size based on pipeline type.
   */
  val effectiveBatchSize: Int
    get() = if (useParallelPipeline) parallelBatchSize else batchSize
}

