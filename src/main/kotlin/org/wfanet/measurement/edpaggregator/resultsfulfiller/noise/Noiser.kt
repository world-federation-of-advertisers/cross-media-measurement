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
import org.wfanet.measurement.eventdataprovider.noiser.DpParams
import org.wfanet.measurement.eventdataprovider.noiser.GaussianNoiser

/**
 * A class for adding noise to measurement results.
 *
 * @param noiseMechanism The direct noise mechanism to use.
 * @param random The random number generator to use.
 */
class Noiser(private val noiseMechanism: DirectNoiseMechanism, private val random: SecureRandom) {
  init {
    // Check if the noise mechanism is supported
    if (!SUPPORTED_NOISE_MECHANISMS.contains(noiseMechanism)) {
      throw RequisitionRefusalException.Default(
        Requisition.Refusal.Justification.SPEC_INVALID,
        "Unsupported noise mechanism: $noiseMechanism",
      )
    }
  }

  /**
   * Gets a noiser for the given privacy parameters.
   *
   * @param privacyParams The differential privacy parameters.
   * @return A noiser for the given parameters.
   */
  fun getNoiser(privacyParams: DifferentialPrivacyParams): AbstractNoiser =
    if (noiseMechanism == DirectNoiseMechanism.CONTINUOUS_GAUSSIAN) {
      GaussianNoiser(DpParams(privacyParams.epsilon, privacyParams.delta), random)
    } else {
      throw RequisitionRefusalException.Default(
        Requisition.Refusal.Justification.SPEC_INVALID,
        "Unsupported noise mechanism: $noiseMechanism",
      )
    }

  /**
   * Add noise to an integer value.
   *
   * @param value The value to add noise to.
   * @param privacyParams Differential privacy params.
   * @return Noised non-negative value.
   */
  fun addNoise(value: Int, privacyParams: DifferentialPrivacyParams): Int {
    val noiser: AbstractNoiser = getNoiser(privacyParams)
    return max(0, value + noiser.sample().toInt())
  }

  /**
   * Add noise to a frequency map.
   *
   * @param frequencyMap The frequency map to add noise to.
   * @param privacyParams Differential privacy params for frequency map.
   * @param reachValue The total reach to use for scaling the frequency map.
   * @return Noised non-negative frequency map.
   */
  fun addNoise(
    frequencyMap: Map<Int, Double>,
    privacyParams: DifferentialPrivacyParams,
    reachValue: Int,
  ): Map<Int, Double> {
    val noiser: AbstractNoiser = getNoiser(privacyParams)

    // Add noise to the histogram and cap negative values to zeros.
    val frequencyHistogram: Map<Int, Int> =
      frequencyMap.mapValues { (_, percentage) ->
        // Round the noise for privacy.
        val noisedCount: Int =
          (percentage * reachValue).roundToInt() + (noiser.sample()).roundToInt()
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
    // The supported noise mechanisms in order of preference.
    private val SUPPORTED_NOISE_MECHANISMS = setOf(DirectNoiseMechanism.CONTINUOUS_GAUSSIAN)
  }
}
