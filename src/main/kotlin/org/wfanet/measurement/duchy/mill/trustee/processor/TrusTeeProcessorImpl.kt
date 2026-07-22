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

import org.wfanet.measurement.computation.DeterministicTruncatedLaplaceNoise
import org.wfanet.measurement.computation.DifferentialPrivacyParams
import org.wfanet.measurement.computation.HistogramComputations
import org.wfanet.measurement.computation.ReachAndFrequencyComputations
import org.wfanet.measurement.computation.ResultMinimumThresholds
import org.wfanet.measurement.computation.SampledReachAndFrequency
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

    // For deterministic truncated-Laplace noise, drop a contribution whose own reach is below the
    // min_users threshold before it enters the aggregate, so its marginal cannot be recovered by
    // differencing overlapping regions. The dropped vector is treated as all-zeros.
    if (isDeterministicTruncatedLaplace && isBelowUserThreshold(vector)) {
      return
    }

    for (i in vector.indices) {
      val frequency = vector[i].toInt()
      require(frequency >= 0) {
        "Invalid frequency value in byte array: $frequency. Frequency must be non-negative."
      }
      currentVector[i] = (currentVector[i] + frequency).coerceAtMost(maxFrequency)
    }
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
    var sampledReachAndFrequency = SampledReachAndFrequency(rawHistogram.sum(), rawHistogram)

    return when (val params = trusTeeParams) {
      is TrusTeeReachParams -> {
        if (isDeterministicTruncatedLaplace) {
          sampledReachAndFrequency =
            applyDeterministicNoise(
              sampledReachAndFrequency,
              DeterministicTruncatedLaplaceNoise.fingerprint(frequencyVector),
              reachDpParams = params.dpParams,
              frequencyDpParams = params.dpParams,
              truncationBound = params.truncationBound,
            )
        }
        val reach =
          ReachAndFrequencyComputations.computeReach(
            sampledReachAndFrequency,
            vidSamplingIntervalWidth,
            frequencyVector.size,
            dpParamsForCompute(params.dpParams),
            resultMinimumThresholds = resultMinimumThresholds,
          )
        ReachResult(reach = reach, methodology = DeterministicMethodology)
      }
      is TrusTeeReachAndFrequencyParams -> {
        if (isDeterministicTruncatedLaplace) {
          sampledReachAndFrequency =
            applyDeterministicNoise(
              sampledReachAndFrequency,
              DeterministicTruncatedLaplaceNoise.fingerprint(frequencyVector),
              reachDpParams = params.reachDpParams,
              frequencyDpParams = params.frequencyDpParams,
              truncationBound = params.truncationBound,
            )
        }
        val reach =
          ReachAndFrequencyComputations.computeReach(
            sampledReachAndFrequency,
            vidSamplingIntervalWidth,
            frequencyVector.size,
            dpParamsForCompute(params.reachDpParams),
            resultMinimumThresholds = resultMinimumThresholds,
          )
        val frequency =
          ReachAndFrequencyComputations.computeFrequencyDistribution(
            sampledReachAndFrequency.frequencyHistogram,
            maxFrequency,
            dpParamsForCompute(params.frequencyDpParams),
            resultMinimumThresholds = resultMinimumThresholds,
            vidSamplingIntervalWidth = vidSamplingIntervalWidth,
          )
        ReachAndFrequencyResult(reach, frequency, DeterministicMethodology)
      }
    }
  }

  /**
   * Returns [sampled] noised with deterministic truncated-Laplace. Reach and frequency draw from
   * [reachDpParams] / [frequencyDpParams] respectively (for a reach-only measurement both are the
   * single reach params).
   */
  private fun applyDeterministicNoise(
    sampled: SampledReachAndFrequency,
    fingerprint: ByteArray,
    reachDpParams: InternalDifferentialPrivacyParams?,
    frequencyDpParams: InternalDifferentialPrivacyParams?,
    truncationBound: Int,
  ): SampledReachAndFrequency =
    DeterministicTruncatedLaplaceNoise.noise(
      sampled,
      fingerprint,
      reachEpsilon = requireNotNull(reachDpParams) { REACH_DP_PARAMS_REQUIRED }.epsilon,
      frequencyEpsilon = requireNotNull(frequencyDpParams) { FREQUENCY_DP_PARAMS_REQUIRED }.epsilon,
      sensitivity = TRUNCATED_LAPLACE_SENSITIVITY,
      truncationBound = truncationBound,
    )

  /**
   * DP params to pass to the compute functions. For deterministic truncated-Laplace, the histogram
   * is already noised (see [DeterministicTruncatedLaplaceNoise.noise]), so noise is turned off with
   * null; otherwise the internal params are converted for the Gaussian path.
   */
  private fun dpParamsForCompute(
    internalDpParams: InternalDifferentialPrivacyParams?
  ): DifferentialPrivacyParams? =
    if (isDeterministicTruncatedLaplace) null else internalDpParams?.toDifferentialPrivacyParams()

  private fun InternalDifferentialPrivacyParams.toDifferentialPrivacyParams():
    DifferentialPrivacyParams {
    return DifferentialPrivacyParams(epsilon, delta)
  }

  companion object Factory : TrusTeeProcessor.Factory {
    // Per-bucket sensitivity for the deterministic truncated-Laplace draws: adding or removing one
    // user changes a histogram bucket count by 1, matching the L-infinity sensitivity the Gaussian
    // path uses.
    private const val TRUNCATED_LAPLACE_SENSITIVITY = 1.0
    private const val REACH_DP_PARAMS_REQUIRED =
      "Reach DP params are required for DETERMINISTIC_TRUNCATED_LAPLACE noise."
    private const val FREQUENCY_DP_PARAMS_REQUIRED =
      "Frequency DP params are required for DETERMINISTIC_TRUNCATED_LAPLACE noise."

    override fun create(trusTeeParams: TrusTeeParams): TrusTeeProcessor {
      return TrusTeeProcessorImpl(trusTeeParams)
    }
  }
}
