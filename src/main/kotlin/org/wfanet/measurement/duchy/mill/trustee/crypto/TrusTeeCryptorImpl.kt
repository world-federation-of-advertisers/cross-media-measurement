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

package org.wfanet.measurement.duchy.mill.trustee.crypto

import com.google.privacy.differentialprivacy.GaussianNoise
import kotlin.math.min
import kotlin.properties.Delegates
import org.wfanet.measurement.api.v2alpha.DifferentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.duchy.utils.ComputationResult
import org.wfanet.measurement.duchy.utils.ReachAndFrequencyResult
import org.wfanet.measurement.duchy.utils.ReachResult
import org.wfanet.measurement.measurementconsumer.stats.TrusTeeMethodology

/** A concrete, stateful implementation of [TrusTeeCryptor]. */
class TrusTeeCryptorImpl(override val measurementSpec: MeasurementSpec) : TrusTeeCryptor {
  /**
   * Holds the aggregated frequency vector.
   *
   * This is initialized with a copy of the first vector passed to [addFrequencyVector]. Subsequent
   * calls add to this vector. It is null until the first vector is added.
   */
  private var aggregatedFrequencyVector: IntArray? = null

  private var maxFrequency by Delegates.notNull<Int>()

  init {
    require(measurementSpec.hasReachAndFrequency() || measurementSpec.hasReach()) {
      "TrusTEE only supports Reach and Reach & Frequency measurements"
    }

    maxFrequency =
      if (measurementSpec.hasReachAndFrequency()) measurementSpec.reachAndFrequency.maximumFrequency
      else 1

    require(maxFrequency >= 1) { "Invalid max frequency: $maxFrequency" }
  }

  override fun addFrequencyVector(vector: IntArray) {
    require(vector.isNotEmpty()) { "Input frequency vector cannot be empty." }

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

    return when {
      measurementSpec.hasReachAndFrequency() -> {
        val reachAndFrequencyParams = measurementSpec.reachAndFrequency

        val reachDpParams =
          if (reachAndFrequencyParams.hasReachPrivacyParams()) {
            reachAndFrequencyParams.reachPrivacyParams
          } else {
            null
          }

        val freqDpParams =
          if (reachAndFrequencyParams.hasFrequencyPrivacyParams()) {
            reachAndFrequencyParams.frequencyPrivacyParams
          } else {
            null
          }

        val reach = computeReach(rawHistogram, reachDpParams)
        val frequency = computeFrequencyDistribution(rawHistogram, freqDpParams)

        ReachAndFrequencyResult(
          reach = reach,
          frequency = frequency,
          methodology = TrusTeeMethodology(frequencyVector.size.toLong()),
        )
      }
      measurementSpec.hasReach() -> {
        val reachParams = measurementSpec.reach
        val reachDpParams = if (reachParams.hasPrivacyParams()) reachParams.privacyParams else null

        val reach = computeReach(rawHistogram, reachDpParams)

        ReachResult(reach = reach, methodology = TrusTeeMethodology(frequencyVector.size.toLong()))
      }
      else -> error("Unsupported measurement spec type")
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
      return rawReach
    }

    val noise = GaussianNoise()
    val noisedReach = noise.addNoise(rawReach, 1, 1L, dpParams.epsilon, dpParams.delta)

    return if (noisedReach < 0) 0L else noisedReach
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
      return emptyMap()
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

  companion object Factory : TrusTeeCryptor.Factory {
    override fun create(measurementSpec: MeasurementSpec): TrusTeeCryptor {
      return TrusTeeCryptorImpl(measurementSpec)
    }
  }
}
