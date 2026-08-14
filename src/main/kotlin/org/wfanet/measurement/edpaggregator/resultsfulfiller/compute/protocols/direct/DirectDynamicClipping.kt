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

import org.wfanet.measurement.computation.DeterministicTruncatedLaplaceParams
import org.wfanet.measurement.computation.DeterministicTruncatedLaplaceResultNoiser
import org.wfanet.measurement.computation.DynamicallyClippedImpressions
import org.wfanet.measurement.computation.FrequencyVectorSeededNoiseSource
import org.wfanet.measurement.computation.ImpressionComputations
import org.wfanet.measurement.computation.ResultMinimumThresholds
import org.wfanet.measurement.eventdataprovider.differentialprivacy.StandardNormalNoiseSource
import org.wfanet.measurement.eventdataprovider.differentialprivacy.StochasticStandardNormalNoiseSource
import org.wfanet.measurement.eventdataprovider.noiser.DirectNoiseMechanism
import org.wfanet.measurement.eventdataprovider.noiser.DpParams
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.AcdpParamsConverter

/**
 * A Direct measurement is produced by a single EDP, so every draw is seeded from one contribution.
 */
private const val DIRECT_CONTRIBUTION_COUNT = 1

/**
 * Separates the clip search's draws from the result draws taken against the same frequency vector.
 */
private const val CLIP_SEARCH_DOMAIN = 1

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
 * The count itself is mechanism-agnostic. What the mechanism decides is where the charge comes from
 * and whether the draws reproduce:
 * - [DirectNoiseMechanism.CONTINUOUS_GAUSSIAN] charges the measurement's own privacy params and
 *   draws fresh randomness, which is the composition the clip search was written against.
 * - [DirectNoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE] charges the params compiled into this
 *   image, so a measurement consumer cannot widen them, and seeds the draws from the frequency
 *   vector so a re-run yields the same clip and the same count. The draws stay Gaussian, which
 *   leaves the calibration, the stopping rule and the remaining-charge weighting as analyzed.
 *
 * @param dpParams the measurement's privacy params, unused by the deterministic mechanism.
 */
fun computeDirectDynamicallyClippedImpressions(
  directNoiseMechanism: DirectNoiseMechanism,
  frequencyData: IntArray,
  dpParams: DpParams,
  vidSamplingIntervalWidth: Double,
  resultMinimumThresholds: ResultMinimumThresholds?,
): DynamicallyClippedImpressions {
  val queryDpParams: DpParams
  val noiseSource: StandardNormalNoiseSource
  when (directNoiseMechanism) {
    DirectNoiseMechanism.CONTINUOUS_GAUSSIAN -> {
      queryDpParams = dpParams
      noiseSource = StochasticStandardNormalNoiseSource()
    }
    DirectNoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE -> {
      queryDpParams =
        DpParams(
          DeterministicTruncatedLaplaceParams.EPSILON,
          DeterministicTruncatedLaplaceParams.DELTA,
        )
      noiseSource =
        FrequencyVectorSeededNoiseSource(
          fingerprint =
            DeterministicTruncatedLaplaceResultNoiser.fingerprint(
              frequencyData,
              DIRECT_CONTRIBUTION_COUNT,
            ),
          domain = CLIP_SEARCH_DOMAIN,
        )
    }
    DirectNoiseMechanism.NONE,
    DirectNoiseMechanism.CONTINUOUS_LAPLACE ->
      throw IllegalArgumentException(
        "$directNoiseMechanism does not support dynamic impression capping"
      )
  }

  return ImpressionComputations.computeDynamicallyClippedImpressionCount(
    frequencyVector = frequencyData,
    queryRho = AcdpParamsConverter.getDirectAcdpCharge(queryDpParams, BAR_SENSITIVITY).rho,
    noiseSource = noiseSource,
    vidSamplingIntervalWidth = vidSamplingIntervalWidth,
    resultMinimumThresholds = resultMinimumThresholds,
  )
}
