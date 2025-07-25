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

/**
 * Configuration for event filtering within the processing pipeline.
 * 
 * This class encapsulates the settings and specifications needed to
 * configure how events are filtered during processing.
 */
data class FilterConfiguration(
  val filterSpecs: List<FilterSpec>,
  val enableParallelFiltering: Boolean = true,
  val filteringThreadPoolSize: Int = 4
) {
  
  /**
   * Creates a FilterProcessor from this configuration.
   * 
   * @return A new FilterProcessor configured with these specifications
   */
  fun createProcessor(): FilterProcessor {
    return FilterProcessor(filterSpecs)
  }
  
  /**
   * Checks if filtering is enabled (has any filter specifications).
   * 
   * @return true if there are filter specifications to apply
   */
  fun isFilteringEnabled(): Boolean = filterSpecs.isNotEmpty()
}