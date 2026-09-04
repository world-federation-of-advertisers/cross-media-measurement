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

import org.wfanet.measurement.computation.DeterministicTruncatedLaplaceResultNoiser
import org.wfanet.measurement.computation.DifferentialPrivacyParams
import org.wfanet.measurement.computation.GaussianResultNoiser
import org.wfanet.measurement.computation.HistogramComputations
import org.wfanet.measurement.computation.NoNoise
import org.wfanet.measurement.computation.ReachAndFrequency
import org.wfanet.measurement.computation.ReachAndFrequencyComputations
import org.wfanet.measurement.computation.ResultMinimumThresholds
import org.wfanet.measurement.computation.ResultNoiser
import org.wfanet.measurement.duchy.utils.ComputationResult
import org.wfanet.measurement.duchy.utils.ReachAndFrequencyResult
import org.wfanet.measurement.duchy.utils.ReachResult
import org.wfanet.measurement.internal.duchy.DifferentialPrivacyParams as InternalDifferentialPrivacyParams
import org.wfanet.measurement.internal.duchy.NoiseMechanism
import org.wfanet.measurement.measurementconsumer.stats.DeterministicMethodology

/** A concrete, stateful implementation of [TrusTeeProcessor]. */
class TrusTeeProcessorImpl(override val trusTeeParams: TrusTeeParams) : TrusTeeProcessor {
  /**
   * Holds the aggregated frequency vector.
   *
   * This is initialized on the first call to [addFrequencyVectorBytes]. Subsequent calls add to
   * this vector.
   */
  private lateinit var aggregatedFrequencyVector: IntArray

  /**
   * Number of contributions aggregated so far, after input suppression. Folded into the noise seed
   * so adding or removing a contribution reseeds the noise even when the aggregate is unchanged.
   */
  private var contributionCount: Int = 0

  private val maxFrequency: Int
  private val vidSamplingIntervalWidth: Double
  private val resultMinimumThresholds: ResultMinimumThresholds?

  private val isDeterministicTruncatedLaplace: Boolean =
    trusTeeParams.noiseMechanism == NoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE

  init {
    when (trusTeeParams) {
      is TrusTeeReachAndFrequencyParams -> {
        maxFrequency = trusTeeParams.maximumFrequency
        require(maxFrequency in 2..Byte.MAX_VALUE) { "Invalid max frequency: $maxFrequency" }
        vidSamplingIntervalWidth = trusTeeParams.vidSamplingIntervalWidth
        resultMinimumThresholds = trusTeeParams.resultMinimumThresholds
      }
      is TrusTeeReachParams -> {
        maxFrequency = 1
        vidSamplingIntervalWidth = trusTeeParams.vidSamplingIntervalWidth
        resultMinimumThresholds = trusTeeParams.resultMinimumThresholds
      }
    }

    // A vidSamplingIntervalWidth of 0 is invalid as it would cause division by zero.
    require(vidSamplingIntervalWidth > 0.0 && vidSamplingIntervalWidth <= 1.0) {
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

    // Validated before the suppression check below, so a malformed vector fails for every
    // mechanism rather than being silently dropped under deterministic noise.
    val invalidFrequency: Byte? = vector.firstOrNull { it < 0 }
    require(invalidFrequency == null) {
      "Invalid frequency value in byte array: $invalidFrequency. Frequency must be non-negative."
    }

    // For deterministic truncated-Laplace noise, drop a contribution whose own reach is below the
    // min_users threshold before it enters the aggregate, so its marginal cannot be recovered by
    // differencing overlapping regions. The dropped vector is treated as all-zeros.
    if (isDeterministicTruncatedLaplace && isBelowUserThreshold(vector)) {
      return
    }

    for (i in vector.indices) {
      currentVector[i] = (currentVector[i] + vector[i].toInt()).coerceAtMost(maxFrequency)
    }
    contributionCount++
  }

  /**
   * Whether [vector]'s own reach (its positive entries, scaled to the population) is below the
   * `min_users` k-anonymity threshold. False when no thresholds are set.
   */
  private fun isBelowUserThreshold(vector: ByteArray): Boolean {
    val thresholds = resultMinimumThresholds ?: return false
    val directReach = vector.count { it.toInt() > 0 }
    return directReach / vidSamplingIntervalWidth < thresholds.minUsers
  }

  override fun computeResult(): ComputationResult {
    check(::aggregatedFrequencyVector.isInitialized) {
      "addFrequencyVectorBytes must be called before computeResult."
    }
    val frequencyVector = aggregatedFrequencyVector
    val rawHistogram = HistogramComputations.buildHistogram(frequencyVector, maxFrequency)
    val sampledReachAndFrequency = ReachAndFrequency(rawHistogram.sum(), rawHistogram)

    // TODO(world-federation-of-advertisers/cross-media-measurement#4454): Add TrusTEE
    // minimum-threshold correction variance.

    return when (val params = trusTeeParams) {
      is TrusTeeReachParams -> {
        // A reach-only measurement has a single set of DP params, and never draws a frequency
        // bucket: computeReach noises the reach and the threshold only.
        val noiser = noiser(frequencyVector, params.dpParams, params.dpParams)
        val reach =
          ReachAndFrequencyComputations.computeReach(
              sampledReachAndFrequency,
              vidSamplingIntervalWidth,
              frequencyVector.size,
              noiser,
              resultMinimumThresholds = resultMinimumThresholds,
            )
            .value
        ReachResult(reach = reach, methodology = DeterministicMethodology)
      }
      is TrusTeeReachAndFrequencyParams -> {
        val noiser = noiser(frequencyVector, params.reachDpParams, params.frequencyDpParams)
        val reach =
          ReachAndFrequencyComputations.computeReach(
              sampledReachAndFrequency,
              vidSamplingIntervalWidth,
              frequencyVector.size,
              noiser,
              resultMinimumThresholds = resultMinimumThresholds,
            )
            .value
        val frequency =
          ReachAndFrequencyComputations.computeFrequencyDistribution(
              rawHistogram,
              maxFrequency,
              noiser,
              resultMinimumThresholds = resultMinimumThresholds,
              vidSamplingIntervalWidth = vidSamplingIntervalWidth,
            )
            .value
        ReachAndFrequencyResult(reach, frequency, DeterministicMethodology)
      }
    }
  }

  /**
   * Returns the noiser for the configured mechanism.
   *
   * A reach-only measurement passes its single set of params as both, which is safe because it
   * never draws a frequency bucket.
   */
  private fun noiser(
    frequencyVector: IntArray,
    reachDpParams: InternalDifferentialPrivacyParams?,
    frequencyDpParams: InternalDifferentialPrivacyParams?,
  ): ResultNoiser {
    if (isDeterministicTruncatedLaplace) {
      // Privacy params are compiled into the attested image, not taken from the measurement spec.
      return DeterministicTruncatedLaplaceResultNoiser(
        frequencyVector,
        contributionCount,
        maxFrequencyPerUser = resultMinimumThresholds?.reachMaxFrequencyPerUser ?: 1,
      )
    }
    if (reachDpParams == null || frequencyDpParams == null) {
      return NoNoise
    }
    return GaussianResultNoiser(
      reachDpParams.toDifferentialPrivacyParams(),
      frequencyDpParams.toDifferentialPrivacyParams(),
      resultMinimumThresholds?.reachMaxFrequencyPerUser ?: 1,
    )
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
