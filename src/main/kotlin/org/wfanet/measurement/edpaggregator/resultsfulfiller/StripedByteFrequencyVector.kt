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
  private fun computeStatistics(): Pair<Double, Long> {
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
  private fun getTotalCountInternal(): Long {
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
  
  
  // FrequencyVector interface implementation
  override fun getReach(): Long {
    val (_, reach) = computeStatistics()
    return reach
  }

  override fun getAverageFrequency(): Double {
    val (avgFreq, _) = computeStatistics()
    return avgFreq
  }

  override fun getTotalCount(): Long {
    return getTotalCountInternal()
  }

}