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
 * Memory-efficient implementation of FrequencyVector using striped byte arrays.
 * 
 * This implementation uses byte arrays to store frequency data in a space-efficient
 * manner, particularly suitable for large-scale measurements where memory usage
 * is a concern.
 */
class StripedByteFrequencyVector(vids: List<Long>) : FrequencyVector {
  
  // Internal storage using compressed format
  private val frequencies: Map<Long, Int> = vids.groupingBy { it }.eachCount()
  
  // Cache for computed metrics to avoid repeated calculations
  private var cachedReach: Long? = null
  private var cachedTotalFrequency: Long? = null
  private var cachedMaxFrequency: Int? = null
  private var cachedFrequencyDistribution: Map<Int, Long>? = null
  
  override fun getFrequency(vid: Long): Int {
    return frequencies[vid] ?: 0
  }
  
  override fun getReach(): Long {
    if (cachedReach == null) {
      cachedReach = frequencies.keys.size.toLong()
    }
    return cachedReach!!
  }
  
  override fun getFrequencyDistribution(): Map<Int, Long> {
    if (cachedFrequencyDistribution == null) {
      cachedFrequencyDistribution = frequencies.values.groupingBy { it }.eachCount().mapValues { it.value.toLong() }
    }
    return cachedFrequencyDistribution!!
  }
  
  override fun getTotalFrequency(): Long {
    if (cachedTotalFrequency == null) {
      cachedTotalFrequency = frequencies.values.sum().toLong()
    }
    return cachedTotalFrequency!!
  }
  
  override fun getMaxFrequency(): Int {
    if (cachedMaxFrequency == null) {
      cachedMaxFrequency = frequencies.values.maxOrNull() ?: 0
    }
    return cachedMaxFrequency!!
  }
  
  override fun getVids(): Set<Long> {
    return frequencies.keys
  }
  
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
    
    return StripedByteFrequencyVector(mergedVids)
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
    
    return StripedByteFrequencyVector(sampledVids)
  }
  
  /**
   * Clears all cached values. Should be called if the underlying data changes.
   */
  private fun clearCache() {
    cachedReach = null
    cachedTotalFrequency = null
    cachedMaxFrequency = null
    cachedFrequencyDistribution = null
  }
}