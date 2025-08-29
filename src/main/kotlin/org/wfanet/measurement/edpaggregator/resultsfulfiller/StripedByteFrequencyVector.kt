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
 * @param size Number of entries to allocate
 */
class StripedByteFrequencyVector(
  val size: Int,
) : FrequencyVector {

  companion object {
    private const val DEFAULT_STRIPE_COUNT = 1024
    private const val MAX_VALUE = 127
  }

  private val stripeCount = DEFAULT_STRIPE_COUNT

  // same as ceil(size / stripeCount) to avoid losing the last stripe
  // when size is not divisible by stripeCount
  private val stripeSize = (size + stripeCount - 1) / stripeCount
  private val data = ByteArray(size)
  private val locks = Array(stripeCount) { Any() }

  private fun getStripe(index: Int) = index / stripeSize

  /**
   * Increments the frequency count for a given VID index.
   *
   * @param index The index to increment
   */
  override fun incrementByIndex(index: Int) {
    if (index in 0..<size) {
      synchronized(locks[getStripe(index)]) {
        val current = data[index].toInt()
        if (current < MAX_VALUE) {
          data[index] = (current + 1).toByte()
        }
      }
    } else {
      throw IllegalArgumentException("Index must be in range [0, ${size-1}].")
    }
  }

  override fun getByteArray(): ByteArray {
    return data
  }

  /**
   * Thread-safe merge of another StripedByteFrequencyVector into this one.
   *
   * This method safely combines two frequency vectors by adding their counts
   * per index, with saturation at the maximum value. It uses lock striping to
   * maintain thread safety while providing good concurrent performance.
   *
   * @param other The other frequency vector to merge into this one
   * @return This frequency vector after merging
   * @throws IllegalArgumentException if the vectors have different sizes
   */
  fun merge(other: StripedByteFrequencyVector): StripedByteFrequencyVector {
    require(size == other.size) {
      "Cannot merge frequency vectors of different sizes: $size != ${other.size}"
    }

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
