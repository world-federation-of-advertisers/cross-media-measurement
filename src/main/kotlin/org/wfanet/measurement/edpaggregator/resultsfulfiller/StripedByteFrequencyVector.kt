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

import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecValidator.validateVidRangesList
import org.wfanet.measurement.computation.KAnonymityParams

/**
 * Get the number of VIDs represented by a PopulationSpec
 */
val PopulationSpec.size: Long
  get() =
    subpopulationsList.sumOf { subPop ->
      subPop.vidRangesList.sumOf { (it.startVid..it.endVidInclusive).count().toLong() }
    }

/**
 * Thread-safe, memory-efficient frequency vector using striped byte arrays.
 *
 * This implementation:
 * - Uses byte arrays for memory efficiency (max count 255 per VID)
 * - Implements lock striping for high-concurrency performance
 * - Provides O(1) increment operations
 * - Supports concurrent access from multiple threads
 * - Handles VidSamplingInterval with primaryRange and wrappedRange
 *
 * @param populationSpec specification of the population being measured
 * @param measurementSpec a MeasurementSpec that specifies a Reach or ReachAndFrequency
 */
class StripedByteFrequencyVector(
  val populationSpec: PopulationSpec,
  val measurementSpec: MeasurementSpec,
  val kAnonymityParams: KAnonymityParams? = null
) : FrequencyVector {

  companion object {
    private const val DEFAULT_STRIPE_COUNT = 1024
  }

  /** The maximum frequency allowed in the output frequency vector. */
  private val maxValue: Int = if (measurementSpec.hasReachAndFrequency()) {
    measurementSpec.reachAndFrequency.maximumFrequency
  } else if (measurementSpec.hasImpression()) {
    measurementSpec.impression.maximumFrequencyPerUser
  } else if (measurementSpec.hasReach() && kAnonymityParams != null) {
    kAnonymityParams.reachMaxFrequencyPerUser
  } else {
    1
  }

  /**
   * For a non-wrapping sampling interval this is the entire range of indexes in the VidIndexMap
   * that correspond to the sampling interval [start, start+width) For a wrapping sampling interval
   * this is the range of indexes that correspond to the sub-interval [start, 1.0)
   */
  private val primaryRange: IntRange

  private val primaryRangeCount: Int

  /**
   * For a wrapping VidSamplingInterval this is the part of the range than spans [0, width-start)
   */
  private val wrappedRange: IntRange

  /** The size of the frequency vector being managed */
  val size: Int

  init {
    require(maxValue in 1..127) {
      "maxValue must be between 1 and 127"
    }
    val vidSamplingInterval = measurementSpec.vidSamplingInterval
    require(vidSamplingInterval.width > 0 && vidSamplingInterval.width <= 1.0) {
      "MeasurementSpec.VidSamplingInterval.width must be > 0 and <= 1"
    }
    require(vidSamplingInterval.start in 0.0..1.0) {
      "MeasurementSpec.VidSamplingInterval.start must be >= 0 and <= 1"
    }

    validateVidRangesList(populationSpec).getOrThrow()
    val populationSizeLong = populationSpec.size
    require(populationSizeLong > 0 && populationSizeLong < Int.MAX_VALUE) {
      "population size must be > 0 and < Int.MAX_VALUE"
    }
    val populationSize = populationSizeLong.toInt()

    // If we have a wrapping interval globalEndIndex will be larger than the populationSize
    val globalStartIndex = (populationSize * vidSamplingInterval.start).toInt()
    val globalEndIndex =
      (populationSize * (vidSamplingInterval.start + vidSamplingInterval.width)).toInt() - 1
    primaryRange = globalStartIndex..minOf(globalEndIndex, populationSize - 1)
    primaryRangeCount = primaryRange.count()
    wrappedRange =
      if (globalEndIndex >= populationSize) {
        0..(globalEndIndex - populationSize)
      } else {
        IntRange.EMPTY
      }

    // Initialize the frequency vector
    val frequencyVectorSize = primaryRangeCount + wrappedRange.count()
    size = frequencyVectorSize
  }

  private val stripeCount = DEFAULT_STRIPE_COUNT
  private val stripeSize = (size + stripeCount - 1) / stripeCount
  private val data = ByteArray(size)
  private val locks = Array(stripeCount) { Any() }

  private fun getStripe(index: Int) = index / stripeSize

  /**
   * Increments the frequency count for a given VID index.
   *
   * Note: Frequency counts are capped at maxValue to fit in a byte.
   *
   * @param globalIndex The global VID index to increment (will be mapped to local index)
   */
  override fun incrementByIndex(globalIndex: Int) {
    // Check if globalIndex is within our sampling interval
    if (!(globalIndex in primaryRange || globalIndex in wrappedRange)) {
      return // Ignore out of range indexes (non-strict mode)
    }

    // Map global index to local index in our frequency vector
    val localIndex =
      if (globalIndex in primaryRange) {
        globalIndex - primaryRange.first
      } else {
        primaryRangeCount + globalIndex
      }

    if (localIndex in 0..<size) {
      synchronized(locks[getStripe(localIndex)]) {
        val current = data[localIndex].toInt() and 0xFF
        if (current < maxValue) {
          data[localIndex] = (current + 1).toByte()
        }
      }
    }
  }

  override fun getArray(): IntArray {
    val result = IntArray(size)
    for (stripe in 0 until stripeCount) {
      synchronized(locks[stripe]) {
        val start = stripe * stripeSize
        val end = minOf(start + stripeSize, size)
        var j = start
        while (j < end) {
          result[j] = data[j].toInt() and 0xFF
          j++
        }
      }
    }
    return result
  }

  /**
   * Thread-safe merge of another StripedByteFrequencyVector into this one.
   *
   * This method safely combines two frequency vectors by adding their counts
   * per VID, with saturation at the maximum value. It uses lock striping to
   * maintain thread safety while providing good concurrent performance.
   *
   * @param other The other frequency vector to merge into this one
   * @return This frequency vector after merging
   * @throws IllegalArgumentException if the vectors have different sizes or incompatible ranges
   */
  fun merge(other: StripedByteFrequencyVector): StripedByteFrequencyVector {
    require(size == other.size) {
      "Cannot merge frequency vectors of different sizes: $size != ${other.size}"
    }
    require(other.primaryRange == primaryRange) {
      "Primary ranges incompatible. other: ${other.primaryRange} this: ${this.primaryRange}"
    }
    require(other.wrappedRange == wrappedRange) {
      "Wrapped ranges incompatible. other: ${other.wrappedRange} this: ${this.wrappedRange}"
    }

    // Process each stripe with appropriate locking
    for (stripe in 0 until stripeCount) {
      synchronized(locks[stripe]) {
        val start = stripe * stripeSize
        val end = minOf(start + stripeSize, size)
        for (i in start until end) {
          val sum = (data[i].toInt() and 0xFF) + (other.data[i].toInt() and 0xFF)
          data[i] = minOf(sum, maxValue).toByte()
        }
      }
    }

    return this
  }

}
