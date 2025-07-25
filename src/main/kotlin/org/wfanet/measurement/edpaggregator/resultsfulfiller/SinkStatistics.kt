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
 * Statistics collected during event sink processing.
 * 
 * Tracks various metrics about event processing for monitoring
 * and debugging purposes.
 */
data class SinkStatistics(
  val eventsProcessed: Long = 0,
  val eventsFiltered: Long = 0,
  val processingTimeMs: Long = 0,
  val reach: Long = 0,
  val maxFrequency: Int = 0
) {
  
  /**
   * Merges this statistics object with another.
   */
  fun merge(other: SinkStatistics): SinkStatistics {
    return SinkStatistics(
      eventsProcessed = eventsProcessed + other.eventsProcessed,
      eventsFiltered = eventsFiltered + other.eventsFiltered,
      processingTimeMs = processingTimeMs + other.processingTimeMs,
      reach = reach + other.reach,
      maxFrequency = maxOf(maxFrequency, other.maxFrequency)
    )
  }
}