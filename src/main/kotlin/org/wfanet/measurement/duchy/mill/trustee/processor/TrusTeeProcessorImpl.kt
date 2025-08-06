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
   * This is initialized with a copy of the first vector passed to [addFrequencyVectorBytes].
   * Subsequent calls add to this vector. It is null until the first vector is added.
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

    val rawHistogram = buildHistogram(frequencyVector, maxFrequency)

    return when (trusTeeParams) {
      is TrusTeeReachParams -> {
        val reach = computeReach(rawHistogram, frequencyVector.size, trusTeeParams.dpParams)

        ReachResult(reach = reach, methodology = TrusTeeMethodology(frequencyVector.size.toLong()))
      }
      is TrusTeeReachAndFrequencyParams -> {
        val reach = computeReach(rawHistogram, frequencyVector.size, trusTeeParams.reachDpParams)
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
  private fun computeReach(
    rawHistogram: LongArray,
    vectorSize: Int,
    dpParams: DifferentialPrivacyParams?,
  ): Long {
    val maxPossibleScaledReach = (vectorSize.toLong() / vidSamplingIntervalWidth).toLong()

    val reachInSample = rawHistogram.sum() - rawHistogram[0]

    if (dpParams == null) {
      val scaledReach = (reachInSample / vidSamplingIntervalWidth).toLong()
      return min(scaledReach, maxPossibleScaledReach)
    }

    val noise = GaussianNoise()
    val noisedReachInSample =
      noise.addNoise(
        /* x= */ reachInSample,
        /* l0Sensitivity= */ 1,
        /* lInfSensitivity= */ 1L,
        /* epsilon= */ dpParams.epsilon,
        /* delta= */ dpParams.delta,
      )
    val scaledNoisedReach =
      if (noisedReachInSample < 0) 0L else (noisedReachInSample / vidSamplingIntervalWidth).toLong()

    return min(scaledNoisedReach, maxPossibleScaledReach)
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
      val totalSampledUsers = rawHistogram.sum()
      if (totalSampledUsers == 0L) return emptyMap()
      return rawHistogram.withIndex().associate { (freq, count) ->
        freq.toLong() to count.toDouble() / totalSampledUsers
      }
    }

    val noise = GaussianNoise()
    val noisedHistogram =
      rawHistogram.map {
        val noisedValue =
          noise.addNoise(
            /* x= */ it,
            /* l0Sensitivity= */ 1,
            /* lInfSensitivity= */ 1L,
            /* epsilon= */ dpParams.epsilon,
            /* delta= */ dpParams.delta,
          )
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

  companion object Factory : TrusTeeProcessor.Factory {
    override fun create(trusTeeParams: TrusTeeParams): TrusTeeProcessor {
      return TrusTeeProcessorImpl(trusTeeParams)
    }
  }
}
