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

import java.util.concurrent.atomic.AtomicLong
import java.util.logging.Logger
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.eventdataprovider.shareshuffle.v2alpha.VidIndexMap
import org.wfanet.measurement.eventdataprovider.shareshuffle.v2alpha.VidNotFoundException
import org.wfanet.measurement.loadtest.dataprovider.LabeledEvent

/**
 * Frequency vector sink that receives filtered events and updates frequency counts.
 *
 * Each sink corresponds to a specific CEL filter and maintains its own frequency vector.
 * Thread-safe for concurrent access.
 */
class FrequencyVectorSink(
  val sinkId: String,
  val description: String,
  private val vidIndexMap: VidIndexMap
) {
  
  companion object {
    private val logger = Logger.getLogger(FrequencyVectorSink::class.java.name)
  }
  
  private val frequencyVector = StripedByteFrequencyVector(vidIndexMap.size.toInt())
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
    matchedEvents: List<LabeledEvent<TestEvent>>,
    totalProcessed: Int
  ) {
    processedCount.addAndGet(totalProcessed.toLong())
    matchedCount.addAndGet(matchedEvents.size.toLong())
    
    matchedEvents.forEach { event ->
      try {
        val index = vidIndexMap[event.vid]
        frequencyVector.incrementByIndex(index)
      } catch (e: VidNotFoundException) {
        errorCount.incrementAndGet()
        logger.warning("VID not found in index map: ${event.vid} (sink: $sinkId)")
      }
    }
  }
  
  /**
   * Returns current statistics for this sink.
   */
  fun getStatistics(): SinkStatistics {
    val (avgFreq, nonZeroCount) = frequencyVector.computeStatistics()
    val totalFrequency = frequencyVector.getTotalCount()
    
    return SinkStatistics(
      sinkId = sinkId,
      description = description,
      processedEvents = processedCount.get(),
      matchedEvents = matchedCount.get(),
      errorCount = errorCount.get(),
      reach = nonZeroCount,
      totalFrequency = totalFrequency,
      averageFrequency = avgFreq
    )
  }
}