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

/**
 * Processor for applying filters to events.
 * 
 * This class handles the application of filter specifications to events,
 * determining which events should be included in measurements based on
 * CEL expressions and other criteria.
 */
class FilterProcessor(
  private val filterSpecs: List<FilterSpec>
) {
  
  /**
   * Applies all filter specifications to an event.
   * 
   * @param event The labeled event to filter
   * @return true if the event passes all filters, false otherwise
   */
  fun matches(event: LabeledEvent<DynamicMessage>): Boolean {
    return filterSpecs.all { spec ->
      spec.matches(event.event)
    }
  }
  
  /**
   * Gets the filter specifications this processor uses.
   * 
   * @return The list of filter specifications
   */
  fun getFilterSpecs(): List<FilterSpec> = filterSpecs
  
  /**
   * Creates a new FilterProcessor with additional filter specifications.
   * 
   * @param additionalSpecs Additional filter specifications to include
   * @return A new FilterProcessor instance
   */
  fun withAdditionalFilters(additionalSpecs: List<FilterSpec>): FilterProcessor {
    return FilterProcessor(filterSpecs + additionalSpecs)
  }
}