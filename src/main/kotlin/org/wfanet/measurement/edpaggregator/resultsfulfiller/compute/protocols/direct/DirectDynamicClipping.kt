/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.wfanet.measurement.edpaggregator.resultsfulfiller.compute.protocols.direct

import org.wfanet.measurement.computation.DeterministicDynamicClippingNoiseSource
import org.wfanet.measurement.computation.DeterministicTruncatedLaplaceParams
import org.wfanet.measurement.computation.DeterministicTruncatedLaplaceResultNoiser
import org.wfanet.measurement.computation.DynamicallyClippedImpressions
import org.wfanet.measurement.computation.ImpressionComputations
import org.wfanet.measurement.computation.ResultMinimumThresholds
import org.wfanet.measurement.eventdataprovider.differentialprivacy.DynamicClippingNoiseSource
import org.wfanet.measurement.eventdataprovider.noiser.DirectNoiseMechanism
import org.wfanet.measurement.eventdataprovider.noiser.DpParams
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.AcdpParamsConverter

/**
 * A Direct measurement is produced by a single EDP, so every draw is seeded from one contribution.
 */
private const val DIRECT_CONTRIBUTION_COUNT = 1

/**
 * The highest per-user frequency the vector can hold.
 *
 * `StripedByteFrequencyVector` saturates each VID at [Byte.MAX_VALUE], so a clip above it would
 * bound nothing, and a search charging for wider bars would charge for bars no user can reach.
 */
private const val MAX_REPRESENTABLE_FREQUENCY = Byte.MAX_VALUE.toInt()

/**
 * One user moves any single bar of the cumulative histogram by at most one, which is the
 * sensitivity the charge conversion is calibrated to. The clip search scales it up for the number
 * of bars it releases.
 */
private const val BAR_SENSITIVITY = 1.0

/**
 * Counts the impressions in [frequencyData] with a clip derived from its own distribution, for a
 * Direct measurement under [directNoiseMechanism].
 *
 * Only [DirectNoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE] is supported. The clip is chosen from
 * the noised histogram, so a mechanism drawing fresh randomness would pick a different clip for the
 * same frequency vector and return a different count on a re-run. This mechanism charges the params
 * compiled into this image, so a measurement consumer cannot widen them, and seeds the draws from
 * the frequency vector, so the clip and the count reproduce. The draws stay Gaussian, which leaves
 * the calibration, the stopping rule and the remaining-charge weighting as analyzed.
 *
 * @param dpParams the measurement's privacy params, unused here.
 */
fun computeDirectDynamicallyClippedImpressions(
  directNoiseMechanism: DirectNoiseMechanism,
  frequencyData: IntArray,
  dpParams: DpParams,
  vidSamplingIntervalWidth: Double,
  resultMinimumThresholds: ResultMinimumThresholds?,
): DynamicallyClippedImpressions {
  val queryDpParams: DpParams
  val noiseSource: DynamicClippingNoiseSource
  when (directNoiseMechanism) {
    // TODO(world-federation-of-advertisers/cross-media-measurement#4401): Rename this mechanism
    // to DETERMINISTIC_NOISE. It names a privacy regime, compiled-in params and seeded draws,
    // rather than a distribution, and the draws here are Gaussian by design.
    DirectNoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE -> {
      queryDpParams =
        DpParams(
          DeterministicTruncatedLaplaceParams.EPSILON,
          DeterministicTruncatedLaplaceParams.DELTA,
        )
      // TODO(world-federation-of-advertisers/cross-media-measurement#4387): Mix in the
      // EDP-supplied seed component once it exists.
      noiseSource =
        DeterministicDynamicClippingNoiseSource(
          DeterministicTruncatedLaplaceResultNoiser.fingerprint(
            frequencyData,
            DIRECT_CONTRIBUTION_COUNT,
          )
        )
    }
    DirectNoiseMechanism.NONE,
    DirectNoiseMechanism.CONTINUOUS_GAUSSIAN,
    DirectNoiseMechanism.CONTINUOUS_LAPLACE ->
      throw IllegalArgumentException(
        "$directNoiseMechanism does not support dynamic impression capping"
      )
  }

  return ImpressionComputations.computeDynamicallyClippedImpressionCount(
    frequencyVector = frequencyData,
    queryRho = AcdpParamsConverter.getDirectAcdpCharge(queryDpParams, BAR_SENSITIVITY).rho,
    maxFrequency = MAX_REPRESENTABLE_FREQUENCY,
    noiseSource = noiseSource,
    vidSamplingIntervalWidth = vidSamplingIntervalWidth,
    resultMinimumThresholds = resultMinimumThresholds,
  )
}
