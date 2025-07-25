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
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.measurementconsumer.stats.VidSamplingInterval

/**
 * Configuration for the event processing pipeline.
 *
 * This data class encapsulates all configuration parameters needed to run
 * the pipeline, making it easier to pass configuration between components.
 */
data class PipelineConfiguration(
  // Pipeline processing configuration
  val batchSize: Int,
  val channelCapacity: Int,
  val threadPoolSize: Int,
  val workers: Int,

  // Storage event source configuration
  val eventSource: EventSource,

  // Population spec file path
  // TODO: Switch to fetch population spec from the API instead of loading from file
  val populationSpecPath: String,
  val vidSamplingInterval: VidSamplingInterval,

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
    require(batchSize > 0) { "Batch size must be positive" }
    require(channelCapacity > 0) { "Channel capacity must be positive" }
    require(workers > 0) { "Workers must be positive" }
    require(threadPoolSize > 0) { "Thread pool size must be positive" }
    require(populationSpecPath.isNotEmpty()) { "Population spec path must not be empty" }
  }

}

