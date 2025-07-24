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

import java.security.SecureRandom
import org.wfanet.measurement.eventdataprovider.shareshuffle.v2alpha.VidIndexMap

/**
 * Implementation of FrequencyVector backed by StripedByteFrequencyVector.
 * 
 * This implementation provides the interface methods while leveraging the
 * efficient striped byte array storage.
 */
class StripedFrequencyVector(
  private val stripedVector: StripedByteFrequencyVector,
  private val vidIndexMap: VidIndexMap
) : FrequencyVector {
  
  override fun getFrequency(vid: Long): Int {
    return try {
      val index = vidIndexMap[vid]
      stripedVector.getFrequencyByIndex(index)
    } catch (e: Exception) {
      0
    }
  }
  
  override fun getReach(): Long {
    val (_, reach) = stripedVector.computeStatistics()
    return reach
  }
  
  override fun getFrequencyDistribution(): Map<Int, Long> {
    return stripedVector.getFrequencyDistribution()
  }
  
  override fun getTotalFrequency(): Long {
    return stripedVector.getTotalCount()
  }
  
  override fun getMaxFrequency(): Int {
    return stripedVector.getMaxFrequency()
  }
  
  override fun getVids(): Set<Long> {
    // Note: This implementation cannot return actual VIDs without reverse mapping
    // from index to VID. This is a limitation of the current VidIndexMap interface.
    // For now, return empty set as VIDs are not needed for reach/frequency computation.
    // If VID retrieval is needed, VidIndexMap interface would need to be extended.
    return emptySet()
  }
  
  override fun merge(other: FrequencyVector): FrequencyVector {
    require(other is StripedFrequencyVector) { 
      "Can only merge with another StripedFrequencyVector" 
    }
    require(vidIndexMap.size == other.vidIndexMap.size) {
      "Cannot merge frequency vectors with different VID index map sizes"
    }
    
    val mergedVector = StripedByteFrequencyVector(vidIndexMap.size.toInt())
    
    // Copy this vector's data
    for (index in stripedVector.getNonZeroIndices()) {
      val freq = stripedVector.getFrequencyByIndex(index)
      for (i in 0 until freq) {
        mergedVector.incrementByIndex(index)
      }
    }
    
    // Add other vector's data
    for (index in other.stripedVector.getNonZeroIndices()) {
      val freq = other.stripedVector.getFrequencyByIndex(index)
      for (i in 0 until freq) {
        mergedVector.incrementByIndex(index)
      }
    }
    
    return StripedFrequencyVector(mergedVector, vidIndexMap)
  }
  
  override fun sample(rate: Double, random: SecureRandom): FrequencyVector {
    require(rate in 0.0..1.0) { "Sampling rate must be between 0 and 1" }
    
    val sampledVector = StripedByteFrequencyVector(vidIndexMap.size.toInt())
    
    for (index in stripedVector.getNonZeroIndices()) {
      val freq = stripedVector.getFrequencyByIndex(index)
      var sampledFreq = 0
      
      // Sample each impression independently
      for (i in 0 until freq) {
        if (random.nextDouble() < rate) {
          sampledFreq++
        }
      }
      
      // Add sampled frequency
      for (i in 0 until sampledFreq) {
        sampledVector.incrementByIndex(index)
      }
    }
    
    return StripedFrequencyVector(sampledVector, vidIndexMap)
  }
}