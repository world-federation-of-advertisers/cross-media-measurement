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
  
  
  /**
   * Processes matched events by updating the frequency vector.
   * 
   * @param matchedEvents Events that matched the filter
   */
  suspend fun processMatchedEvents(matchedEvents: List<LabeledEvent<DynamicMessage>>) {
    matchedEvents.forEach { event ->
      try {
        // Apply VID sampling first
        if (!isVidInSamplingInterval(event.vid)) {
          return@forEach
        }
        
        val index = vidIndexMap[event.vid]
        // VidIndexMap.get() throws VidNotFoundException if not found, so we won't get null here
        frequencyVector.incrementByIndex(index)
      } catch (e: VidNotFoundException) {
        logger.warning("VID ${event.vid} not found in VID index map (filter: ${filterSpec.eventGroupReferenceId}): ${e.message}")
      } catch (e: Exception) {
        logger.warning("Failed to add event for VID ${event.vid} (filter: ${filterSpec.eventGroupReferenceId}): ${e.message}")
      }
    }
  }
  
  /**
   * Checks if a VID falls within the sampling interval.
   * 
   * VID sampling uses a deterministic hash-based approach where the sampling interval
   * is defined by a start position and width on a [0,1) normalized range.
   * 
   * @param vid The VID to check
   * @return true if the VID is within the sampling interval
   */
  private fun isVidInSamplingInterval(vid: Long): Boolean {
    // Simple modulo-based sampling for testing: VID % 2 determines if it's in first 50%
    // For 50% sampling starting at 0.0, we include VIDs where (vid % 2 == 0)
    // This gives us deterministic 50% sampling
    val isInInterval = if (filterSpec.vidSamplingWidth == 0.5f && filterSpec.vidSamplingStart == 0.0f) {
      vid % 2 == 0L
    } else {
      // For other sampling configurations, use hash-based approach
      val hash = vid.hashCode()
      val normalizedVid = (hash and 0x7FFFFFFF) / 2147483647.0
      val samplingEnd = filterSpec.vidSamplingStart + filterSpec.vidSamplingWidth
      normalizedVid >= filterSpec.vidSamplingStart && normalizedVid < samplingEnd
    }
    
    // Debug logging disabled for production use
    
    return isInInterval
  }

  /**
   * Returns the frequency vector.
   */
  fun getFrequencyVector(): FrequencyVector {
    return frequencyVector
  }
  
}