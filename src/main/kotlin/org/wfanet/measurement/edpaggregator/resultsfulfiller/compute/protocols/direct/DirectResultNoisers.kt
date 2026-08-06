/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.resultsfulfiller.compute.protocols.direct

import org.wfanet.measurement.api.v2alpha.ProtocolConfig.NoiseMechanism
import org.wfanet.measurement.computation.DeterministicTruncatedLaplaceResultNoiser
import org.wfanet.measurement.computation.DifferentialPrivacyParams
import org.wfanet.measurement.computation.GaussianResultNoiser
import org.wfanet.measurement.computation.NoNoise
import org.wfanet.measurement.computation.ResultNoiser
import org.wfanet.measurement.eventdataprovider.noiser.DirectNoiseMechanism

/**
 * A Direct measurement is produced by a single EDP, so every draw is seeded from one contribution.
 */
private const val DIRECT_CONTRIBUTION_COUNT = 1

/**
 * The system's privacy parameters for DETERMINISTIC_TRUNCATED_LAPLACE, compiled into the EDP
 * Aggregator image rather than set by the measurement consumer. epsilon = 1 with a truncation bound
 * of 8 encodes delta = 1/1000.
 */
private const val DETERMINISTIC_EPSILON = 1.0
private const val DETERMINISTIC_TRUNCATION_BOUND = 8

/**
 * Returns the [ResultNoiser] for [directNoiseMechanism].
 *
 * Reach and the impression threshold draw from [reachDpParams]; frequency buckets draw from
 * [frequencyDpParams]. A measurement that releases no frequency distribution may pass the same
 * value for both. DETERMINISTIC_TRUNCATED_LAPLACE ignores both and uses the compiled system params.
 *
 * @param frequencyData the raw frequency vector, which seeds the deterministic mechanism.
 */
fun buildDirectResultNoiser(
  directNoiseMechanism: DirectNoiseMechanism,
  frequencyData: IntArray,
  reachDpParams: DifferentialPrivacyParams,
  frequencyDpParams: DifferentialPrivacyParams,
  maxFrequencyPerUser: Int,
): ResultNoiser =
  when (directNoiseMechanism) {
    DirectNoiseMechanism.NONE -> NoNoise
    DirectNoiseMechanism.CONTINUOUS_GAUSSIAN ->
      GaussianResultNoiser(reachDpParams, frequencyDpParams, maxFrequencyPerUser)
    DirectNoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE ->
      DeterministicTruncatedLaplaceResultNoiser(
        combinedFrequencyVector = frequencyData,
        contributionCount = DIRECT_CONTRIBUTION_COUNT,
        reachEpsilon = DETERMINISTIC_EPSILON,
        frequencyEpsilon = DETERMINISTIC_EPSILON,
        truncationBound = DETERMINISTIC_TRUNCATION_BOUND,
        maxFrequencyPerUser = maxFrequencyPerUser,
      )
    DirectNoiseMechanism.CONTINUOUS_LAPLACE ->
      throw IllegalArgumentException("$directNoiseMechanism is not supported for Direct results")
  }

/** Returns the [NoiseMechanism] stamped on a result noised with [this]. */
fun DirectNoiseMechanism.toProtocolConfigNoiseMechanism(): NoiseMechanism =
  when (this) {
    DirectNoiseMechanism.NONE -> NoiseMechanism.NONE
    DirectNoiseMechanism.CONTINUOUS_LAPLACE -> NoiseMechanism.CONTINUOUS_LAPLACE
    DirectNoiseMechanism.CONTINUOUS_GAUSSIAN -> NoiseMechanism.CONTINUOUS_GAUSSIAN
    DirectNoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE ->
      NoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE
  }
