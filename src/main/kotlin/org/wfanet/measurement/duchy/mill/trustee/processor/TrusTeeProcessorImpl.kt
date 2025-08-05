// Copyright 2025 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.duchy.mill.trustee.processor

import com.google.privacy.differentialprivacy.GaussianNoise
import kotlin.math.min
import kotlin.properties.Delegates
import org.wfanet.measurement.duchy.utils.ComputationResult
import org.wfanet.measurement.duchy.utils.ReachAndFrequencyResult
import org.wfanet.measurement.duchy.utils.ReachResult
import org.wfanet.measurement.internal.duchy.DifferentialPrivacyParams
import org.wfanet.measurement.measurementconsumer.stats.TrusTeeMethodology

/** A concrete, stateful implementation of [TrusTeeProcessor]. */
class TrusTeeProcessorImpl(override val trusTeeParams: TrusTeeParams) : TrusTeeProcessor {
  /**
   * Holds the aggregated frequency vector.
   *
   * This is initialized with a copy of the first vector passed to [addFrequencyVector]. Subsequent
   * calls add to this vector. It is null until the first vector is added.
   */
  private var aggregatedFrequencyVector: IntArray? = null

  private var maxFrequency by Delegates.notNull<Int>()

  private var vidSamplingIntervalWidth by Delegates.notNull<Float>()

  init {
    when (trusTeeParams) {
      is TrusTeeReachAndFrequencyParams -> {
        maxFrequency = trusTeeParams.maximumFrequency
        require(maxFrequency >= 2) { "Invalid max frequency: $maxFrequency" }
        vidSamplingIntervalWidth = trusTeeParams.vidSamplingIntervalWidth
      }
      is TrusTeeReachParams -> {
        maxFrequency = 1
        vidSamplingIntervalWidth = trusTeeParams.vidSamplingIntervalWidth
      }
    }

    require(vidSamplingIntervalWidth >= 0.0f && vidSamplingIntervalWidth <= 1.0f) {
      "Invalid vid sampling interval width: $vidSamplingIntervalWidth"
    }
  }

  override fun addFrequencyVectorBytes(bytes: ByteArray) {
    require(bytes.isNotEmpty()) { "Input frequency vector cannot be empty." }

    val vector = bytes.toIntArray()

    if (aggregatedFrequencyVector == null) {
      aggregatedFrequencyVector = IntArray(vector.size)
    }

    val currentVector = requireNotNull(aggregatedFrequencyVector)
    require(vector.size == currentVector.size) {
      "Input vector size ${vector.size} does not match expected size ${currentVector.size}"
    }

    for (i in currentVector.indices) {
      currentVector[i] = min(currentVector[i] + vector[i], maxFrequency)
    }
  }

  override fun computeResult(): ComputationResult {
    val frequencyVector =
      aggregatedFrequencyVector
        ?: throw IllegalStateException("addFrequencyVector must be called before computeResult.")

    val rawHistogram = buildHistogram(frequencyVector)

    return when (trusTeeParams) {
      is TrusTeeReachParams -> {
        val reach = computeReach(rawHistogram, trusTeeParams.dpParams)

        ReachResult(reach = reach, methodology = TrusTeeMethodology(frequencyVector.size.toLong()))
      }
      is TrusTeeReachAndFrequencyParams -> {
        val reach = computeReach(rawHistogram, trusTeeParams.reachDpParams)
        val frequency = computeFrequencyDistribution(rawHistogram, trusTeeParams.frequencyDpParams)

        ReachAndFrequencyResult(
          reach = reach,
          frequency = frequency,
          methodology = TrusTeeMethodology(frequencyVector.size.toLong()),
        )
      }
    }
  }

  /**
   * Computes the reach, applying differential privacy noise if specified.
   *
   * @param rawHistogram The raw, non-DP histogram of frequencies.
   * @param dpParams The privacy parameters for the reach computation. If null, the raw reach is
   *   returned.
   * @return The reach value, potentially with noise.
   */
  private fun computeReach(rawHistogram: LongArray, dpParams: DifferentialPrivacyParams?): Long {
    val rawReach = rawHistogram.sum() - rawHistogram[0]
    if (dpParams == null) {
      return (rawReach / vidSamplingIntervalWidth).toLong()
    }

    val noise = GaussianNoise()
    val noisedReach = noise.addNoise(rawReach, 1, 1L, dpParams.epsilon, dpParams.delta)

    return if (noisedReach < 0) 0L else (noisedReach / vidSamplingIntervalWidth).toLong()
  }

  /**
   * Computes the frequency distribution, applying differential privacy noise if specified.
   *
   * @param rawHistogram The raw, non-DP histogram of frequencies.
   * @param dpParams The privacy parameters for the frequency computation. If null, the raw
   *   distribution is returned.
   * @return A map representing the frequency distribution, potentially with noise.
   */
  private fun computeFrequencyDistribution(
    rawHistogram: LongArray,
    dpParams: DifferentialPrivacyParams?,
  ): Map<Long, Double> {
    if (dpParams == null) {
      val totalUsers = rawHistogram.sum()
      if (totalUsers == 0L) return emptyMap()
      return rawHistogram.withIndex().associate { (freq, count) ->
        freq.toLong() to count.toDouble() / totalUsers
      }
    }

    val noise = GaussianNoise()
    val noisedHistogram =
      rawHistogram.map {
        val noisedValue = noise.addNoise(it, 1, 1L, dpParams.epsilon, dpParams.delta)
        if (noisedValue < 0) 0L else noisedValue
      }

    val totalNoisedCount = noisedHistogram.sum()
    if (totalNoisedCount == 0L) {
      return (0..maxFrequency).associate { it.toLong() to 0.0 }
    }

    return noisedHistogram.withIndex().associate { (freq, count) ->
      freq.toLong() to count.toDouble() / totalNoisedCount
    }
  }

  private fun buildHistogram(frequencyVector: IntArray): LongArray {
    val histogram = LongArray(maxFrequency + 1)
    for (userFrequency in frequencyVector) {
      histogram[userFrequency]++
    }
    return histogram
  }

  private fun ByteArray.toIntArray(): IntArray {
    return this.map { byte ->
        val frequency = byte.toInt()
        require(frequency >= 0 && frequency < 127) {
          "Invalid frequency value in byte array: $frequency. Must be non-negative and less than 127."
        }
        frequency
      }
      .toIntArray()
  }

  companion object Factory : TrusTeeProcessor.Factory {
    override fun create(trusTeeParams: TrusTeeParams): TrusTeeProcessor {
      return TrusTeeProcessorImpl(trusTeeParams)
    }
  }
}
