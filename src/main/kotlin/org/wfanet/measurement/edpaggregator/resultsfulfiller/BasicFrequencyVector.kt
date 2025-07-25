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

/**
 * Basic implementation of FrequencyVector for simple use cases.
 * 
 * This is a straightforward implementation suitable for scenarios where
 * performance and memory optimization are not critical concerns.
 * 
 * @param vids List of VIDs to include in the frequency vector
 */
class BasicFrequencyVector(vids: List<Long>) : FrequencyVector {
  
  private val frequencies: Map<Long, Int> = vids.groupingBy { it }.eachCount()
  
  override fun getFrequency(vid: Long): Int = frequencies[vid] ?: 0
  
  override fun getReach(): Long = frequencies.keys.size.toLong()
  
  override fun getFrequencyDistribution(): Map<Int, Long> {
    return frequencies.values.groupingBy { it }.eachCount().mapValues { it.value.toLong() }
  }
  
  override fun getTotalFrequency(): Long = frequencies.values.sum().toLong()
  
  override fun getMaxFrequency(): Int = frequencies.values.maxOrNull() ?: 0
  
  override fun getVids(): Set<Long> = frequencies.keys
  
  override fun merge(other: FrequencyVector): FrequencyVector {
    val mergedVids = mutableListOf<Long>()
    
    // Add all VIDs from this vector
    frequencies.forEach { (vid, count) ->
      repeat(count) { mergedVids.add(vid) }
    }
    
    // Add all VIDs from other vector
    other.getVids().forEach { vid ->
      val count = other.getFrequency(vid)
      repeat(count) { mergedVids.add(vid) }
    }
    
    return BasicFrequencyVector(mergedVids)
  }
  
  override fun sample(rate: Double, random: SecureRandom): FrequencyVector {
    val sampledVids = mutableListOf<Long>()
    
    frequencies.forEach { (vid, count) ->
      repeat(count) {
        if (random.nextDouble() < rate) {
          sampledVids.add(vid)
        }
      }
    }
    
    return BasicFrequencyVector(sampledVids)
  }
}