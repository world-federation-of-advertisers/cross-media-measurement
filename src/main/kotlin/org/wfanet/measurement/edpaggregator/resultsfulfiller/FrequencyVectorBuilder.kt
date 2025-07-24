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

import org.wfanet.measurement.eventdataprovider.shareshuffle.v2alpha.VidIndexMap
import org.wfanet.measurement.eventdataprovider.shareshuffle.v2alpha.VidNotFoundException

/**
 * Builder interface for constructing frequency vectors during event processing.
 */
interface FrequencyVectorBuilder {
  /** Adds an event occurrence for the specified VID. */
  fun addEvent(vid: Long)
  
  /** Builds and returns the final FrequencyVector. */
  fun build(): FrequencyVector
}

/**
 * Builder implementation for StripedFrequencyVector.
 * 
 * @property vidIndexMap The VID to index mapping
 */
class StripedFrequencyVectorBuilder(
  private val vidIndexMap: VidIndexMap
) : FrequencyVectorBuilder {
  
  private val stripedVector = StripedByteFrequencyVector(vidIndexMap.size.toInt())
  
  override fun addEvent(vid: Long) {
    try {
      val index = vidIndexMap[vid]
      stripedVector.incrementByIndex(index)
    } catch (e: VidNotFoundException) {
      // Ignore VIDs not in the index map
    }
  }
  
  override fun build(): FrequencyVector {
    return StripedFrequencyVector(stripedVector, vidIndexMap)
  }
}