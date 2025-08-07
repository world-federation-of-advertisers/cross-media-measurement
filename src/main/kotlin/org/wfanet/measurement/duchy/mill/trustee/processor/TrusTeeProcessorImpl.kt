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

import kotlin.math.min
import org.wfanet.measurement.duchy.utils.ComputationResult
import org.wfanet.measurement.duchy.utils.ReachAndFrequencyResult
import org.wfanet.measurement.duchy.utils.ReachResult
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

  private val maxFrequency: Int
  private val vidSamplingIntervalWidth: Float

  init {
    when (trusTeeParams) {
      is TrusTeeReachAndFrequencyParams -> {
        maxFrequency = trusTeeParams.maximumFrequency
        require(maxFrequency >= 2 && maxFrequency <= Byte.MAX_VALUE) {
          "Invalid max frequency: $maxFrequency"
        }
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

  override fun addFrequencyVectorBytes(bytes: ByteArray) {
    require(bytes.isNotEmpty()) { "Input frequency vector cannot be empty." }

    if (aggregatedFrequencyVector == null) {
      aggregatedFrequencyVector = IntArray(bytes.size)
    }

    val currentVector = requireNotNull(aggregatedFrequencyVector)
    require(bytes.size == currentVector.size) {
      "Input vector size ${bytes.size} does not match expected size ${currentVector.size}"
    }

    for (i in bytes.indices) {
      val frequency = bytes[i].toInt()
      require(frequency >= 0) {
        "Invalid frequency value in byte array: $frequency. Frequency must be non-negative."
      }
      currentVector[i] = min(currentVector[i] + frequency, maxFrequency)
    }
  }

  override fun computeResult(): ComputationResult {
    val frequencyVector =
      aggregatedFrequencyVector
        ?: throw IllegalStateException(
          "addFrequencyVectorBytes must be called before computeResult."
        )

    val rawHistogram = ReachAndFrequencyCalculator.buildHistogram(frequencyVector, maxFrequency)

    return when (trusTeeParams) {
      is TrusTeeReachParams -> {
        val reach =
          ReachAndFrequencyCalculator.computeReach(
            rawHistogram,
            frequencyVector.size,
            vidSamplingIntervalWidth,
            trusTeeParams.dpParams,
          )

        ReachResult(reach = reach, methodology = TrusTeeMethodology(frequencyVector.size.toLong()))
      }
      is TrusTeeReachAndFrequencyParams -> {
        val reach =
          ReachAndFrequencyCalculator.computeReach(
            rawHistogram,
            frequencyVector.size,
            vidSamplingIntervalWidth,
            trusTeeParams.reachDpParams,
          )
        val frequency =
          ReachAndFrequencyCalculator.computeFrequencyDistribution(
            rawHistogram,
            maxFrequency,
            trusTeeParams.frequencyDpParams,
          )

        ReachAndFrequencyResult(
          reach = reach,
          frequency = frequency,
          methodology = TrusTeeMethodology(frequencyVector.size.toLong()),
        )
      }
    }
  }

  companion object Factory : TrusTeeProcessor.Factory {
    override fun create(trusTeeParams: TrusTeeParams): TrusTeeProcessor {
      return TrusTeeProcessorImpl(trusTeeParams)
    }
  }
}
