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

import org.wfanet.measurement.computation.DifferentialPrivacyParams
import org.wfanet.measurement.computation.HistogramComputations
import org.wfanet.measurement.computation.ReachAndFrequencyComputations
import org.wfanet.measurement.duchy.utils.ComputationResult
import org.wfanet.measurement.duchy.utils.ReachAndFrequencyResult
import org.wfanet.measurement.duchy.utils.ReachResult
import org.wfanet.measurement.internal.duchy.DifferentialPrivacyParams as InternalDifferentialPrivacyParams
import org.wfanet.measurement.measurementconsumer.stats.TrusTeeMethodology

/** A concrete, stateful implementation of [TrusTeeProcessor]. */
class TrusTeeProcessorImpl(override val trusTeeParams: TrusTeeParams) : TrusTeeProcessor {
  /**
   * Holds the aggregated frequency vector.
   *
   * This is initialized on the first call to [addFrequencyVectorBytes]. Subsequent calls add to
   * this vector.
   */
  private lateinit var aggregatedFrequencyVector: IntArray

  private val maxFrequency: Int
  private val vidSamplingIntervalWidth: Float

  init {
    when (trusTeeParams) {
      is TrusTeeReachAndFrequencyParams -> {
        maxFrequency = trusTeeParams.maximumFrequency
        require(maxFrequency in 2..Byte.MAX_VALUE) { "Invalid max frequency: $maxFrequency" }
        vidSamplingIntervalWidth = trusTeeParams.vidSamplingIntervalWidth
      }
      is TrusTeeReachParams -> {
        maxFrequency = 1
        vidSamplingIntervalWidth = trusTeeParams.vidSamplingIntervalWidth
      }
    }

    // A vidSamplingIntervalWidth of 0 is invalid as it would cause division by zero.
    require(vidSamplingIntervalWidth > 0.0f && vidSamplingIntervalWidth <= 1.0f) {
      "Invalid vid sampling interval width: $vidSamplingIntervalWidth"
    }
  }

  override fun addFrequencyVector(vector: ByteArray) {
    require(vector.isNotEmpty()) { "Input frequency vector cannot be empty." }

    if (!::aggregatedFrequencyVector.isInitialized) {
      aggregatedFrequencyVector = IntArray(vector.size)
    }

    val currentVector = aggregatedFrequencyVector
    require(vector.size == currentVector.size) {
      "Input vector size ${vector.size} does not match expected size ${currentVector.size}"
    }

    for (i in vector.indices) {
      val frequency = vector[i].toInt()
      require(frequency >= 0) {
        "Invalid frequency value in byte array: $frequency. Frequency must be non-negative."
      }
      currentVector[i] = (currentVector[i] + frequency).coerceAtMost(maxFrequency)
    }
  }

  override fun computeResult(): ComputationResult {
    check(::aggregatedFrequencyVector.isInitialized) {
      "addFrequencyVectorBytes must be called before computeResult."
    }
    val frequencyVector = aggregatedFrequencyVector

    val rawHistogram = HistogramComputations.buildHistogram(frequencyVector, maxFrequency)

    return when (trusTeeParams) {
      is TrusTeeReachParams -> {
        val reach =
          ReachAndFrequencyComputations.computeReach(
            rawHistogram,
            vidSamplingIntervalWidth,
            frequencyVector.size,
            trusTeeParams.dpParams.toDifferentialPrivacyParams(),
            kAnonymityParams = null,
          )

        ReachResult(reach = reach, methodology = TrusTeeMethodology(frequencyVector.size.toLong()))
      }
      is TrusTeeReachAndFrequencyParams -> {
        val reach =
          ReachAndFrequencyComputations.computeReach(
            rawHistogram,
            vidSamplingIntervalWidth,
            frequencyVector.size,
            trusTeeParams.reachDpParams.toDifferentialPrivacyParams(),
            kAnonymityParams = null,
          )
        val frequency =
          ReachAndFrequencyComputations.computeFrequencyDistribution(
            rawHistogram,
            maxFrequency,
            trusTeeParams.frequencyDpParams.toDifferentialPrivacyParams(),
            kAnonymityParams = null,
            vidSamplingIntervalWidth = null,
          )

        ReachAndFrequencyResult(reach, frequency, TrusTeeMethodology(frequencyVector.size.toLong()))
      }
    }
  }

  private fun InternalDifferentialPrivacyParams.toDifferentialPrivacyParams():
    DifferentialPrivacyParams {
    return DifferentialPrivacyParams(epsilon, delta)
  }

  companion object Factory : TrusTeeProcessor.Factory {
    override fun create(trusTeeParams: TrusTeeParams): TrusTeeProcessor {
      return TrusTeeProcessorImpl(trusTeeParams)
    }
  }
}
