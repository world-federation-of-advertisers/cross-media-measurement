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
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.fold

/**
 * Sink that collects events into frequency vectors.
 * 
 * This class consumes streams of filtered events and builds frequency vectors
 * for reach and frequency measurements, applying any necessary noise or
 * differential privacy mechanisms.
 */
class FrequencyVectorSink(
  private val filterConfiguration: FilterConfiguration
) {
  
  private val builder = FrequencyVectorBuilder()
  private var statistics = SinkStatistics()
  
  /**
   * Processes a flow of events and builds a frequency vector.
   * 
   * @param events The flow of labeled events to process
   * @return The resulting frequency vector
   */
  suspend fun process(events: Flow<LabeledEvent<DynamicMessage>>): FrequencyVector {
    val startTime = System.currentTimeMillis()
    val processor = filterConfiguration.createProcessor()
    
    val processedCount = events.fold(0L) { count, event ->
      val passed = if (filterConfiguration.isFilteringEnabled()) {
        processor.matches(event)
      } else {
        true
      }
      
      if (passed) {
        builder.addVid(event.vid)
        count + 1
      } else {
        statistics = statistics.copy(eventsFiltered = statistics.eventsFiltered + 1)
        count
      }
    }
    
    val endTime = System.currentTimeMillis()
    val frequencyVector = builder.buildOptimal()
    
    statistics = statistics.copy(
      eventsProcessed = processedCount,
      processingTimeMs = endTime - startTime,
      reach = frequencyVector.getReach(),
      maxFrequency = frequencyVector.getMaxFrequency()
    )
    
    return frequencyVector
  }
  
  /**
   * Gets the processing statistics for this sink.
   * 
   * @return The current sink statistics
   */
  fun getStatistics(): SinkStatistics = statistics
  
  /**
   * Resets the sink for reuse.
   */
  fun reset() {
    builder.clear()
    statistics = SinkStatistics()
  }
}