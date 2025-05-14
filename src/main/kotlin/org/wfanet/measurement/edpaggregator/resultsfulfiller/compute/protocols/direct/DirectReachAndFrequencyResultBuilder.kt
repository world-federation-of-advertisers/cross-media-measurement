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

package org.wfanet.measurement.edpaggregator.resultsfulfiller.compute.protocols.direct

import kotlin.math.max
import kotlin.math.roundToInt
import kotlinx.coroutines.flow.Flow
import org.apache.commons.math3.distribution.ConstantRealDistribution
import org.wfanet.measurement.api.v2alpha.DeterministicCountDistinct
import org.wfanet.measurement.api.v2alpha.DeterministicDistribution
import org.wfanet.measurement.api.v2alpha.DifferentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.frequency
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.reach
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.NoiseMechanism
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.dataprovider.MeasurementResults
import org.wfanet.measurement.dataprovider.RequisitionRefusalException
import org.wfanet.measurement.edpaggregator.resultsfulfiller.compute.MeasurementResultBuilder
import org.wfanet.measurement.eventdataprovider.noiser.AbstractNoiser
import org.wfanet.measurement.eventdataprovider.noiser.DirectNoiseMechanism
import org.wfanet.measurement.eventdataprovider.noiser.DpParams
import org.wfanet.measurement.eventdataprovider.noiser.GaussianNoiser
import org.wfanet.measurement.eventdataprovider.noiser.LaplaceNoiser
import java.security.SecureRandom
import java.util.logging.Logger

/**
 * Builder for direct reach and frequency measurement results.
 *
 * @param directProtocolConfig The direct protocol configuration.
 * @param sampledVids The sampled vids.
 * @param maxFrequency The maximum frequency to consider.
 * @param reachPrivacyParams The differential privacy parameters for reach.
 * @param frequencyPrivacyParams The differential privacy parameters for frequency.
 * @param samplingRate The sampling rate used to sample the events.
 * @param directNoiseMechanism The direct noise mechanism to use.
 * @param random The random number generator to use.
 */
class DirectReachAndFrequencyResultBuilder(
  private val directProtocolConfig: ProtocolConfig.Direct,
  private val sampledVids: Flow<Long>,
  private val maxFrequency: Int,
  private val reachPrivacyParams: DifferentialPrivacyParams,
  private val frequencyPrivacyParams: DifferentialPrivacyParams,
  private val samplingRate: Float,
  private val directNoiseMechanism: DirectNoiseMechanism = DirectNoiseMechanism.CONTINUOUS_GAUSSIAN,
  private val random: SecureRandom = SecureRandom(),
) : MeasurementResultBuilder {
  private val logger: Logger = Logger.getLogger(this::class.java.name)

  /**
   * Builds a non-noisy reach and frequency measurement result.
   *
   * @return The non-noisy reach and frequency measurement result.
   */
  override fun buildMeasurementResult(): Measurement.Result {
    if (!directProtocolConfig.hasDeterministicCountDistinct()) {
      throw RequisitionRefusalException.Default(
        Requisition.Refusal.Justification.DECLINED,
        "No valid methodologies for direct reach computation.",
      )
    }
    if (!directProtocolConfig.hasDeterministicDistribution()) {
      throw RequisitionRefusalException.Default(
        Requisition.Refusal.Justification.DECLINED,
        "No valid methodologies for direct frequency distribution computation.",
      )
    }

    val (sampledReachValue, frequencyMap) =
      MeasurementResults.computeReachAndFrequency(
        sampledVids,
        maxFrequency,
      )

    val scaledReachValue = (sampledReachValue / samplingRate).toLong()

    return MeasurementKt.result {
      reach = reach {
        value = scaledReachValue
        this.noiseMechanism = NoiseMechanism.NONE
        deterministicCountDistinct = DeterministicCountDistinct.getDefaultInstance()
      }
      frequency = frequency {
        relativeFrequencyDistribution.putAll(frequencyMap.mapKeys { it.key.toLong() })
        this.noiseMechanism = NoiseMechanism.NONE
        deterministicDistribution = DeterministicDistribution.getDefaultInstance()
      }
    }
  }

  /**
   * Builds a noisy reach and frequency measurement result.
   *
   * @return The noisy reach and frequency measurement result.
   */
  override fun buildNoisyMeasurementResult(): Measurement.Result {
    if (!directProtocolConfig.hasDeterministicCountDistinct()) {
      throw RequisitionRefusalException.Default(
        Requisition.Refusal.Justification.DECLINED,
        "No valid methodologies for direct reach computation.",
      )
    }
    if (!directProtocolConfig.hasDeterministicDistribution()) {
      throw RequisitionRefusalException.Default(
        Requisition.Refusal.Justification.DECLINED,
        "No valid methodologies for direct frequency distribution computation.",
      )
    }

    val (sampledReachValue, frequencyMap) =
      MeasurementResults.computeReachAndFrequency(
        sampledVids,
        maxFrequency,
      )

    logger.info("Adding $directNoiseMechanism publisher noise to direct reach and frequency...")
    val noiseBuilder = ReachAndFrequencyNoiseBuilder(
      sampledReachValue,
      directNoiseMechanism,
      random
    )
    val sampledNoisedReachValue =
      noiseBuilder.addReachPublisherNoise(reachPrivacyParams)
    val noisedFrequencyMap =
      noiseBuilder.addFrequencyPublisherNoise(frequencyMap, frequencyPrivacyParams)

    val scaledNoisedReachValue =
      (sampledNoisedReachValue / samplingRate).toLong()

    val protocolConfigNoiseMechanism = when (directNoiseMechanism) {
      DirectNoiseMechanism.NONE -> NoiseMechanism.NONE
      DirectNoiseMechanism.CONTINUOUS_LAPLACE -> NoiseMechanism.CONTINUOUS_LAPLACE
      DirectNoiseMechanism.CONTINUOUS_GAUSSIAN -> NoiseMechanism.CONTINUOUS_GAUSSIAN
    }

    return MeasurementKt.result {
      reach = reach {
        value = scaledNoisedReachValue
        this.noiseMechanism = protocolConfigNoiseMechanism
        deterministicCountDistinct = DeterministicCountDistinct.getDefaultInstance()
      }
      frequency = frequency {
        relativeFrequencyDistribution.putAll(noisedFrequencyMap.mapKeys { it.key.toLong() })
        this.noiseMechanism = protocolConfigNoiseMechanism
        deterministicDistribution = DeterministicDistribution.getDefaultInstance()
      }
    }
  }

  private fun getPublisherNoiser(
    privacyParams: DifferentialPrivacyParams,
    directNoiseMechanism: DirectNoiseMechanism,
    random: SecureRandom,
  ): AbstractNoiser =
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

  /**
   * Add publisher noise to calculated direct reach.
   *
   * @param reachValue Direct reach value.
   * @param privacyParams Differential privacy params for reach.
   * @param directNoiseMechanism Selected noise mechanism for direct reach.
   * @param random The random number generator to use.
   * @return Noised non-negative reach value.
   */
  private fun addReachPublisherNoise(
    reachValue: Int,
    privacyParams: DifferentialPrivacyParams,
    directNoiseMechanism: DirectNoiseMechanism,
    random: SecureRandom,
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
   * @param random The random number generator to use.
   * @return Noised non-negative frequency map.
   */
  private fun addFrequencyPublisherNoise(
    reachValue: Int,
    frequencyMap: Map<Int, Double>,
    privacyParams: DifferentialPrivacyParams,
    directNoiseMechanism: DirectNoiseMechanism,
    random: SecureRandom,
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
} 
