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

/**
 * Thread-safe, memory-efficient frequency vector using striped byte arrays.
 *
 * This implementation:
 * - Uses byte arrays for memory efficiency (max count 127 per VID)
 * - Implements lock striping for high-concurrency performance
 * - Provides O(1) increment operations
 * - Supports concurrent access from multiple threads
 * - Tracks total uncapped impressions for direct measurement fulfillment
 *
 * @param size Number of entries to allocate
 */
class StripedByteFrequencyVector(val size: Int, val stripeCount: Int = DEFAULT_STRIPE_COUNT) {

  companion object {
    private const val DEFAULT_STRIPE_COUNT = 1024
    private const val MAX_VALUE = Byte.MAX_VALUE.toInt()
  }

  // same as ceil(size / stripeCount) to avoid losing the last stripe
  // when size is not divisible by stripeCount
  private val stripeSize = (size + stripeCount - 1) / stripeCount
  private val data = ByteArray(size)
  private val locks = Array(stripeCount) { Any() }

  /** Counter for total impressions without any capping, used for uncapped direct measurements. */
  private val totalUncappedImpressions = AtomicLong(0L)

  private fun getStripe(index: Int) = index / stripeSize

  /**
   * Increments the frequency count for a given VID index.
   *
   * Also increments the total uncapped impressions counter.
   *
   * @param index The index to increment
   */
  fun increment(index: Int) {
    require(index in 0 until size) { "Index must be in range [0, ${size - 1}]" }
    totalUncappedImpressions.incrementAndGet()
    synchronized(locks[getStripe(index)]) {
      val current = data[index].toInt()
      if (current < MAX_VALUE) {
        data[index] = (current + 1).toByte()
      }
    }
  }

  fun getByteArray(): ByteArray {
    synchronized(locks) {
      return data.clone()
    }
  }

  /** Returns the total count of impressions without any frequency capping applied. */
  fun getTotalUncappedImpressions(): Long {
    return totalUncappedImpressions.get()
  }

  /**
   * Thread-safe merge of another StripedByteFrequencyVector into this one.
   *
   * This method safely combines two frequency vectors by adding their counts per index, with
   * saturation at the maximum value. It uses lock striping to maintain thread safety while
   * providing good concurrent performance. Also merges the total uncapped impressions counter.
   *
   * Note: While individual AtomicLong operations are atomic, the merge operation as a whole is not
   * atomic with respect to concurrent increments. However, this is acceptable since merges are
   * typically performed at aggregation boundaries after concurrent processing has completed.
   *
   * @param other The other frequency vector to merge into this one
   * @return This frequency vector after merging
   * @throws IllegalArgumentException if the vectors have different sizes
   */
  fun merge(other: StripedByteFrequencyVector): StripedByteFrequencyVector {
    require(size == other.size) {
      "Cannot merge frequency vectors of different sizes: $size != ${other.size}"
    }

    // Merge uncapped impressions counter
    totalUncappedImpressions.addAndGet(other.totalUncappedImpressions.get())

    // Process each stripe with appropriate locking
    for (stripe in 0 until stripeCount) {
      synchronized(locks[stripe]) {
        val start = stripe * stripeSize
        val end = minOf(start + stripeSize, size)
        for (i in start until end) {
          val sum = data[i].toInt() + other.data[i].toInt()
          data[i] = minOf(sum, MAX_VALUE).toByte()
        }
      }
    }

    return this
  }
}
