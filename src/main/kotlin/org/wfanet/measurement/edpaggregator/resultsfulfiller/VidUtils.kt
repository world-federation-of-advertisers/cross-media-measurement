// Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.protobuf.Message
import kotlin.math.max
import kotlin.math.roundToInt
import kotlin.random.Random
import kotlin.random.asJavaRandom
import org.apache.commons.math3.distribution.ConstantRealDistribution
import org.wfanet.anysketch.crypto.ElGamalPublicKey as AnySketchElGamalPublicKey
import org.wfanet.anysketch.crypto.elGamalPublicKey as anySketchElGamalPublicKey
import org.wfanet.measurement.api.v2alpha.DifferentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.ElGamalPublicKey
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.NoiseMechanism
import org.wfanet.measurement.eventdataprovider.noiser.AbstractNoiser
import org.wfanet.measurement.eventdataprovider.noiser.DirectNoiseMechanism
import org.wfanet.measurement.eventdataprovider.noiser.DpParams
import org.wfanet.measurement.eventdataprovider.noiser.GaussianNoiser
import org.wfanet.measurement.eventdataprovider.noiser.LaplaceNoiser
import org.wfanet.measurement.loadtest.common.sampleVids
import org.wfanet.measurement.loadtest.dataprovider.EventQuery

/**
 * Utility functions for VID-related operations in the EDP aggregator.
 */
object VidUtils {
  /**
   * Samples VIDs from multiple [EventQuery.EventGroupSpec]s with a sampling interval.
   */
  fun sampleVids(
    eventQuery: EventQuery<Message>,
    eventGroupSpecs: Iterable<EventQuery.EventGroupSpec>,
    vidSamplingIntervalStart: Float,
    vidSamplingIntervalWidth: Float,
  ): Iterable<Long> {
    return org.wfanet.measurement.loadtest.common.sampleVids(
      eventQuery,
      eventGroupSpecs,
      vidSamplingIntervalStart,
      vidSamplingIntervalWidth
    )
  }

  /**
   * Gets a publisher noiser based on privacy parameters and noise mechanism.
   */
  fun getPublisherNoiser(
    privacyParams: DifferentialPrivacyParams,
    directNoiseMechanism: DirectNoiseMechanism,
    random: Random,
  ): AbstractNoiser =
    when (directNoiseMechanism) {
      DirectNoiseMechanism.NONE ->
        object : AbstractNoiser() {
          override val distribution = ConstantRealDistribution(0.0)
          override val variance: Double
            get() = distribution.numericalVariance
        }
      DirectNoiseMechanism.CONTINUOUS_LAPLACE ->
        LaplaceNoiser(DpParams(privacyParams.epsilon, privacyParams.delta), random.asJavaRandom())
      DirectNoiseMechanism.CONTINUOUS_GAUSSIAN ->
        GaussianNoiser(DpParams(privacyParams.epsilon, privacyParams.delta), random.asJavaRandom())
    }

  /**
   * Add publisher noise to calculated direct reach.
   *
   * @param reachValue Direct reach value.
   * @param privacyParams Differential privacy params for reach.
   * @param directNoiseMechanism Selected noise mechanism for direct reach.
   * @return Noised non-negative reach value.
   */
  fun addReachPublisherNoise(
    reachValue: Int,
    privacyParams: DifferentialPrivacyParams,
    directNoiseMechanism: DirectNoiseMechanism,
    random: Random = Random,
  ): Int {
    val reachNoiser: AbstractNoiser =
      getPublisherNoiser(privacyParams, directNoiseMechanism, random)

    return max(0, reachValue + reachNoiser.sample().toInt())
  }

  /**
   * Add publisher noise to calculated direct frequency.
   *
   * @param reachValue Direct reach value.
   * @param frequencyMap Direct frequency.
   * @param privacyParams Differential privacy params for frequency map.
   * @param directNoiseMechanism Selected noise mechanism for direct frequency.
   * @return Noised non-negative frequency map.
   */
  fun addFrequencyPublisherNoise(
    reachValue: Int,
    frequencyMap: Map<Int, Double>,
    privacyParams: DifferentialPrivacyParams,
    directNoiseMechanism: DirectNoiseMechanism,
    random: Random = Random,
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

  /**
   * Selects the most preferred [DirectNoiseMechanism] for reach and frequency measurements from the
   * overlap of a list of preferred [DirectNoiseMechanism] and a set of [DirectNoiseMechanism]
   * [options].
   */
  fun selectReachAndFrequencyNoiseMechanism(
    options: Set<DirectNoiseMechanism>
  ): DirectNoiseMechanism {
    val preferences = DIRECT_MEASUREMENT_ACDP_NOISE_MECHANISM_PREFERENCES

    return preferences.firstOrNull { preference -> options.contains(preference) }
      ?: throw IllegalArgumentException("No valid noise mechanism option for reach or frequency measurements.")
  }

  /**
   * Selects the most preferred [DirectNoiseMechanism] for impression measurements from the overlap
   * of a list of preferred [DirectNoiseMechanism] and a set of [DirectNoiseMechanism] [options].
   */
  fun selectImpressionNoiseMechanism(
    options: Set<DirectNoiseMechanism>
  ): DirectNoiseMechanism {
    val preferences = DIRECT_MEASUREMENT_ACDP_NOISE_MECHANISM_PREFERENCES

    return preferences.firstOrNull { preference -> options.contains(preference) }
      ?: throw IllegalArgumentException("No valid noise mechanism option for impression measurements.")
  }

  /**
   * Selects the most preferred [DirectNoiseMechanism] for watch duration measurements from the
   * overlap of a list of preferred [DirectNoiseMechanism] and a set of [DirectNoiseMechanism]
   * [options].
   */
  fun selectWatchDurationNoiseMechanism(
    options: Set<DirectNoiseMechanism>
  ): DirectNoiseMechanism {
    val preferences = DIRECT_MEASUREMENT_ACDP_NOISE_MECHANISM_PREFERENCES

    return preferences.firstOrNull { preference -> options.contains(preference) }
      ?: throw IllegalArgumentException("No valid noise mechanism option for watch duration measurements.")
  }

  // The direct noise mechanisms for ACDP composition in PBM for direct measurements in order
  // of preference. Currently, ACDP composition only supports CONTINUOUS_GAUSSIAN noise for direct
  // measurements.
  private val DIRECT_MEASUREMENT_ACDP_NOISE_MECHANISM_PREFERENCES =
    listOf(DirectNoiseMechanism.CONTINUOUS_GAUSSIAN)
}

/**
 * Converts a [DirectNoiseMechanism] to a [NoiseMechanism].
 */
fun DirectNoiseMechanism.toProtocolConfigNoiseMechanism(): NoiseMechanism {
  return when (this) {
    DirectNoiseMechanism.NONE -> NoiseMechanism.NONE
    DirectNoiseMechanism.CONTINUOUS_LAPLACE -> NoiseMechanism.CONTINUOUS_LAPLACE
    DirectNoiseMechanism.CONTINUOUS_GAUSSIAN -> NoiseMechanism.CONTINUOUS_GAUSSIAN
  }
}

/**
 * Converts a [NoiseMechanism] to a nullable [DirectNoiseMechanism].
 *
 * @return [DirectNoiseMechanism] when there is a matched, otherwise null.
 */
fun NoiseMechanism.toDirectNoiseMechanism(): DirectNoiseMechanism? {
  return when (this) {
    NoiseMechanism.NONE -> DirectNoiseMechanism.NONE
    NoiseMechanism.CONTINUOUS_LAPLACE -> DirectNoiseMechanism.CONTINUOUS_LAPLACE
    NoiseMechanism.CONTINUOUS_GAUSSIAN -> DirectNoiseMechanism.CONTINUOUS_GAUSSIAN
    NoiseMechanism.NOISE_MECHANISM_UNSPECIFIED,
    NoiseMechanism.GEOMETRIC,
    NoiseMechanism.DISCRETE_GAUSSIAN,
    NoiseMechanism.UNRECOGNIZED -> {
      null
    }
  }
}

/**
 * Converts an [ElGamalPublicKey] to an [AnySketchElGamalPublicKey].
 */
fun ElGamalPublicKey.toAnySketchElGamalPublicKey(): AnySketchElGamalPublicKey {
  val source = this
  return anySketchElGamalPublicKey {
    generator = source.generator
    element = source.element
  }
} 
