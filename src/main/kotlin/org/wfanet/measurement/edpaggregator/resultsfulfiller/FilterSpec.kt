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
import org.wfanet.measurement.api.v2alpha.RequisitionSpec

/**
 * Specification for filtering events using CEL expressions.
 * 
 * This class encapsulates the filtering logic for events based on
 * CEL (Common Expression Language) expressions from requisition specs.
 */
data class FilterSpec(
  val celExpression: String,
  val requisitionSpec: RequisitionSpec
) {
  
  /**
   * Checks if this filter spec matches the given event.
   * 
   * @param event The event to evaluate
   * @return true if the event matches the filter criteria
   */
  fun matches(event: Message): Boolean {
    // Basic implementation - will be enhanced in later PRs
    return celExpression.isNotEmpty()
  }
}