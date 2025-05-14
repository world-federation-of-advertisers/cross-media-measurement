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
import org.apache.commons.math3.distribution.ConstantRealDistribution
import org.wfanet.measurement.api.v2alpha.DifferentialPrivacyParams
import org.wfanet.measurement.eventdataprovider.noiser.AbstractNoiser
import org.wfanet.measurement.eventdataprovider.noiser.DirectNoiseMechanism
import org.wfanet.measurement.eventdataprovider.noiser.DpParams
import org.wfanet.measurement.eventdataprovider.noiser.GaussianNoiser
import org.wfanet.measurement.eventdataprovider.noiser.LaplaceNoiser
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.NoiseMechanism


/**
 * Abstract class for building noisers.
 */
abstract class NoiseBuilder {
  /**
   * Selects the most preferred [DirectNoiseMechanism] for a measurements from the
   * overlap of a list of preferred [DirectNoiseMechanism] and a set of [DirectNoiseMechanism]
   * [options].
   *
   * @param options The set of available noise mechanism options.
   * @return The selected noise mechanism.
   */
  abstract fun selectNoiseMechanism(options: Set<DirectNoiseMechanism>): DirectNoiseMechanism

  /**
   * Gets a publisher noiser for the given privacy parameters and noise mechanism.
   *
   * @param privacyParams The differential privacy parameters.
   * @param directNoiseMechanism The direct noise mechanism to use.
   * @param random The random number generator to use.
   * @return A noiser for the given parameters.
   */
  fun getPublisherNoiser(
    privacyParams: DifferentialPrivacyParams,
    directNoiseMechanism: DirectNoiseMechanism,
    random: SecureRandom,
  ): AbstractNoiser =
    // TODO: Use a proper DP library instead of the Halo built-in noisers.
    when (directNoiseMechanism) {
      DirectNoiseMechanism.NONE ->
        object : AbstractNoiser() {
          override val distribution = ConstantRealDistribution(0.0)
          override val variance: Double
            get() = distribution.numericalVariance
        }
      DirectNoiseMechanism.CONTINUOUS_LAPLACE ->
        LaplaceNoiser(DpParams(privacyParams.epsilon, privacyParams.delta), random)
      DirectNoiseMechanism.CONTINUOUS_GAUSSIAN ->
        GaussianNoiser(DpParams(privacyParams.epsilon, privacyParams.delta), random)
    }
}
