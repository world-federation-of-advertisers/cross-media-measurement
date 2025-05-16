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

package org.wfanet.measurement.edpaggregator.resultsfulfiller.noise

import java.security.SecureRandom
import kotlin.math.max
import kotlin.math.roundToInt
import org.wfanet.measurement.api.v2alpha.DifferentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.dataprovider.RequisitionRefusalException
import org.wfanet.measurement.eventdataprovider.noiser.AbstractNoiser
import org.wfanet.measurement.eventdataprovider.noiser.DirectNoiseMechanism

/**
 * Noise builder for reach and frequency measurements.
 *
 * @param random The random number generator to use.
 */
class ReachAndFrequencyNoiseBuilder(
    private val reachValue: Int,
    private val directNoiseMechanism: DirectNoiseMechanism,
    private val random: SecureRandom = SecureRandom()) : NoiseBuilder() {
  /**
   * Selects the most preferred [DirectNoiseMechanism] for reach and frequency measurements from the
   * overlap of a list of preferred [DirectNoiseMechanism] and a set of [DirectNoiseMechanism]
   * [options].
   *
   * @param options The set of available noise mechanism options.
   * @return The selected noise mechanism.
   */
  override fun selectNoiseMechanism(
    options: Set<DirectNoiseMechanism>
  ): DirectNoiseMechanism {
    val preferences = DIRECT_MEASUREMENT_ACDP_NOISE_MECHANISM_PREFERENCES

    return preferences.firstOrNull { preference -> options.contains(preference) }
      ?: throw RequisitionRefusalException.Default(
        Requisition.Refusal.Justification.SPEC_INVALID,
        "No valid noise mechanism option for reach or frequency measurements.",
      )
  }

  /**
   * Add publisher noise to calculated direct reach.
   *
   * @param privacyParams Differential privacy params for reach.
   * @return Noised non-negative reach value.
   */
  fun addReachPublisherNoise(
    privacyParams: DifferentialPrivacyParams,
  ): Int {
    val reachNoiser: AbstractNoiser =
      getPublisherNoiser(privacyParams, directNoiseMechanism, random)

    return max(0, reachValue + reachNoiser.sample().toInt())
  }

  /**
   * Add publisher noise to calculated direct frequency.
   *
   * @param frequencyMap Direct frequency.
   * @param privacyParams Differential privacy params for frequency map.
   * @return Noised non-negative frequency map.
   */
  fun addFrequencyPublisherNoise(
    frequencyMap: Map<Int, Double>,
    privacyParams: DifferentialPrivacyParams,
  ): Map<Int, Double> {
    val frequencyNoiser: AbstractNoiser =
      getPublisherNoiser(privacyParams, directNoiseMechanism, random)

    // Add noise to the histogram and cap negative values to zeros.
    val frequencyHistogram: Map<Int, Int> =
      frequencyMap.mapValues { (_, percentage) ->
        // Round the noise for privacy.
        val noisedCount: Int =
          (percentage * reachValue).roundToInt() + (frequencyNoiser.sample()).roundToInt()
        max(0, noisedCount)
      }
    val normalizationTerm: Double = frequencyHistogram.values.sum().toDouble()
    // Normalize to get the distribution
    return if (normalizationTerm != 0.0) {
      frequencyHistogram.mapValues { (_, count) -> count / normalizationTerm }
    } else {
      frequencyHistogram.mapValues { 0.0 }
    }
  }

  companion object {
    // The direct noise mechanisms for ACDP composition in PBM for direct measurements in order
    // of preference. Currently, ACDP composition only supports CONTINUOUS_GAUSSIAN noise for direct
    // measurements.
    private val DIRECT_MEASUREMENT_ACDP_NOISE_MECHANISM_PREFERENCES =
      listOf(DirectNoiseMechanism.CONTINUOUS_GAUSSIAN)
  }
}
