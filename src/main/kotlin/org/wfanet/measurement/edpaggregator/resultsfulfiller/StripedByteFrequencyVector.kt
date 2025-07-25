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
 * Thread-safe, memory-efficient frequency vector using striped byte arrays.
 * 
 * This implementation:
 * - Uses byte arrays for memory efficiency (max count 255 per VID)
 * - Implements lock striping for high-concurrency performance
 * - Provides O(1) increment operations
 * - Supports concurrent access from multiple threads
 * 
 * @param size The total number of VIDs to track
 */
class StripedByteFrequencyVector(private val size: Int) : FrequencyVector {
  
  companion object {
    private const val DEFAULT_STRIPE_COUNT = 1024
    private const val MAX_BYTE_VALUE = 255
  }
  
  private val stripeCount = DEFAULT_STRIPE_COUNT
  private val stripeSize = (size + stripeCount - 1) / stripeCount
  private val data = ByteArray(size)
  private val locks = Array(stripeCount) { Any() }
  
  private fun getStripe(index: Int) = index / stripeSize
  
  /**
   * Increments the frequency count for a given VID index.
   * 
   * Note: Frequency counts are capped at 255 to fit in a byte.
   * 
   * @param index The VID index to increment
   */
  fun incrementByIndex(index: Int) {
    if (index >= 0 && index < size) {
      synchronized(locks[getStripe(index)]) {
        val current = data[index].toInt() and 0xFF
        if (current < MAX_BYTE_VALUE) {
          data[index] = (current + 1).toByte()
        }
      }
    }
  }
  
  /**
   * Computes reach and average frequency statistics.
   * 
   * @return Pair of (average frequency, reach count)
   */
  fun computeStatistics(): Pair<Double, Long> {
    var totalFrequency = 0L
    var nonZeroCount = 0L
    
    for (i in 0 until stripeCount) {
      synchronized(locks[i]) {
        val start = i * stripeSize
        val end = minOf(start + stripeSize, size)
        for (j in start until end) {
          val count = data[j].toInt() and 0xFF
          if (count > 0) {
            totalFrequency += count
            nonZeroCount++
          }
        }
      }
    }
    
    val averageFrequency = if (nonZeroCount > 0) {
      totalFrequency.toDouble() / nonZeroCount
    } else {
      0.0
    }
    
    return Pair(averageFrequency, nonZeroCount)
  }
  
  /**
   * Gets the total frequency count across all VIDs.
   * 
   * @return Total frequency count
   */
  fun getTotalCount(): Long {
    var total = 0L
    for (i in 0 until stripeCount) {
      synchronized(locks[i]) {
        val start = i * stripeSize
        val end = minOf(start + stripeSize, size)
        for (j in start until end) {
          total += data[j].toInt() and 0xFF
        }
      }
    }
    return total
  }
  
  /**
   * Gets the frequency count for a specific index.
   * 
   * @param index The index to query
   * @return Frequency count at the index
   */
  fun getFrequencyByIndex(index: Int): Int {
    if (index < 0 || index >= size) return 0
    synchronized(locks[getStripe(index)]) {
      return data[index].toInt() and 0xFF
    }
  }
  
  /**
   * Gets all indices with non-zero frequency.
   * 
   * @return List of indices with frequency > 0
   */
  fun getNonZeroIndices(): List<Int> {
    val indices = mutableListOf<Int>()
    for (i in 0 until stripeCount) {
      synchronized(locks[i]) {
        val start = i * stripeSize
        val end = minOf(start + stripeSize, size)
        for (j in start until end) {
          if ((data[j].toInt() and 0xFF) > 0) {
            indices.add(j)
          }
        }
      }
    }
    return indices
  }
  
  /**
   * Gets the frequency distribution (frequency value -> count).
   * 
   * @return Map of frequency values to their counts
   */
  override fun getFrequencyDistribution(): Map<Int, Long> {
    val distribution = mutableMapOf<Int, Long>()
    for (i in 0 until stripeCount) {
      synchronized(locks[i]) {
        val start = i * stripeSize
        val end = minOf(start + stripeSize, size)
        for (j in start until end) {
          val freq = data[j].toInt() and 0xFF
          if (freq > 0) {
            distribution[freq] = distribution.getOrDefault(freq, 0L) + 1L
          }
        }
      }
    }
    return distribution
  }
  
  /**
   * Gets the maximum frequency value.
   * 
   * @return Maximum frequency across all VIDs
   */
  override fun getMaxFrequency(): Int {
    var max = 0
    for (i in 0 until stripeCount) {
      synchronized(locks[i]) {
        val start = i * stripeSize
        val end = minOf(start + stripeSize, size)
        for (j in start until end) {
          val freq = data[j].toInt() and 0xFF
          if (freq > max) {
            max = freq
          }
        }
      }
    }
    return max
  }

  // FrequencyVector interface implementation
  override fun getFrequency(vid: Long): Int {
    // Note: This implementation assumes VID == index
    // For proper VID mapping, use with a VidIndexMap wrapper
    val index = vid.toInt()
    return getFrequencyByIndex(index)
  }

  override fun getReach(): Long {
    val (_, reach) = computeStatistics()
    return reach
  }

  override fun getTotalFrequency(): Long {
    return getTotalCount()
  }

  override fun getVids(): Set<Long> {
    // Return indices as VIDs for this simple implementation
    return getNonZeroIndices().map { it.toLong() }.toSet()
  }

  override fun merge(other: FrequencyVector): FrequencyVector {
    require(other is StripedByteFrequencyVector) {
      "Can only merge with another StripedByteFrequencyVector"
    }
    require(size == other.size) {
      "Cannot merge frequency vectors with different sizes"
    }

    val mergedVector = StripedByteFrequencyVector(size)

    // Copy this vector's data
    for (index in getNonZeroIndices()) {
      val freq = getFrequencyByIndex(index)
      for (i in 0 until freq) {
        mergedVector.incrementByIndex(index)
      }
    }

    // Add other vector's data
    for (index in other.getNonZeroIndices()) {
      val freq = other.getFrequencyByIndex(index)
      for (i in 0 until freq) {
        mergedVector.incrementByIndex(index)
      }
    }

    return mergedVector
  }

  override fun sample(rate: Double, random: SecureRandom): FrequencyVector {
    require(rate in 0.0..1.0) { "Sampling rate must be between 0 and 1" }

    val sampledVector = StripedByteFrequencyVector(size)

    for (index in getNonZeroIndices()) {
      val freq = getFrequencyByIndex(index)
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

    return sampledVector
  }
}