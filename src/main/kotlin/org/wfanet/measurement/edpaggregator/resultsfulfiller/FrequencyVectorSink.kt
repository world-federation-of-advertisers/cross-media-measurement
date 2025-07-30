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
import java.util.concurrent.atomic.AtomicLong
import java.util.logging.Logger
import org.wfanet.measurement.eventdataprovider.shareshuffle.v2alpha.VidIndexMap
import org.wfanet.measurement.eventdataprovider.shareshuffle.v2alpha.VidNotFoundException

/**
 * Frequency vector sink that receives filtered events and builds frequency vectors.
 *
 * Each sink corresponds to a specific filter specification and maintains its own frequency vector.
 * Thread-safe for concurrent access.
 */
class FrequencyVectorSink(
  val filterSpec: FilterSpec,
  private val frequencyVector: FrequencyVector,
  private val vidIndexMap: VidIndexMap
) {
  
  companion object {
    private val logger = Logger.getLogger(FrequencyVectorSink::class.java.name)
  }
  
  private val processedCount = AtomicLong(0)
  private val matchedCount = AtomicLong(0)
  private val errorCount = AtomicLong(0)
  
  /**
   * Processes matched events by updating the frequency vector.
   * 
   * @param matchedEvents Events that matched the filter
   * @param totalProcessed Total number of events that were processed (including non-matches)
   */
  suspend fun processMatchedEvents(
    matchedEvents: List<LabeledEvent<DynamicMessage>>,
    totalProcessed: Int
  ) {
    processedCount.addAndGet(totalProcessed.toLong())
    matchedCount.addAndGet(matchedEvents.size.toLong())
    
    matchedEvents.forEach { event ->
      try {
        val index = vidIndexMap[event.vid]
        frequencyVector.incrementByIndex(index)
      } catch (e: Exception) {
        errorCount.incrementAndGet()
        logger.warning("Failed to add event for VID ${event.vid} (filter: ${filterSpec.eventGroupReferenceId}): ${e.message}")
      }
    }
  }
  
  /**
   * Returns the frequency vector.
   */
  fun getFrequencyVector(): FrequencyVector {
    return frequencyVector
  }
  
  /**
   * Returns current statistics for this sink.
   */
  fun getStatistics(): SinkStatistics {
    val frequencyVector = getFrequencyVector()
    
    return SinkStatistics(
      sinkId = filterSpec.eventGroupReferenceId,
      description = "Filter: ${filterSpec.celExpression}",
      processedEvents = processedCount.get(),
      matchedEvents = matchedCount.get(),
      errorCount = errorCount.get(),
      reach = frequencyVector.getReach(),
      totalFrequency = frequencyVector.getTotalCount(),
      averageFrequency = frequencyVector.getAverageFrequency()
    )
  }
}