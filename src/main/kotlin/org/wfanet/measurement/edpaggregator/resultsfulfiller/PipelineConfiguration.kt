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
  val batchSize: Int,
  val channelCapacity: Int,
  val threadPoolSize: Int,
  val workers: Int,
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
  }

}

