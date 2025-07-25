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

import java.time.LocalDate
import java.time.ZoneId

/**
 * Configuration for the event processing pipeline.
 * 
 * This class contains all the settings needed to configure how the pipeline
 * processes events, including batch sizes, parallel processing settings,
 * and filtering configurations.
 */
data class PipelineConfiguration(
  val startDate: LocalDate,
  val endDate: LocalDate,
  val batchSize: Int = 10000,
  val channelCapacity: Int = 100,
  val eventSourceType: EventSourceType,
  val filterConfiguration: FilterConfiguration = FilterConfiguration(emptyList()),
  val zoneId: ZoneId = ZoneId.systemDefault(),
  val useParallelPipeline: Boolean = false,
  val parallelBatchSize: Int = 100000,
  val parallelWorkers: Int = Runtime.getRuntime().availableProcessors(),
  val threadPoolSize: Int = Runtime.getRuntime().availableProcessors() * 4,
  val disableLogging: Boolean = false
) {
  
  /**
   * Creates an appropriate pipeline instance based on configuration.
   * 
   * @return EventProcessingPipeline or ParallelBatchedPipeline based on settings
   */
  fun createPipeline(): Any {
    return if (useParallelPipeline) {
      ParallelBatchedPipeline(this)
    } else {
      EventProcessingPipeline(this)
    }
  }
  
  /**
   * Validates the configuration settings.
   * 
   * @throws IllegalArgumentException if configuration is invalid
   */
  fun validate() {
    require(batchSize > 0) { "Batch size must be positive" }
    require(channelCapacity > 0) { "Channel capacity must be positive" }
    require(parallelBatchSize > 0) { "Parallel batch size must be positive" }
    require(parallelWorkers > 0) { "Parallel workers must be positive" }
    require(threadPoolSize > 0) { "Thread pool size must be positive" }
    require(!startDate.isAfter(endDate)) { "Start date must not be after end date" }
  }
  
  /**
   * Gets the estimated total processing time based on date range.
   * 
   * @return Estimated processing duration in days
   */
  fun getProcessingDurationDays(): Long {
    return java.time.temporal.ChronoUnit.DAYS.between(startDate, endDate.plusDays(1))
  }
}