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

package org.wfanet.measurement.edpaggregator.resultsfulfiller.compute

import kotlin.math.max
import kotlin.math.roundToInt
import kotlinx.coroutines.flow.Flow
import org.apache.commons.math3.distribution.ConstantRealDistribution
import org.wfanet.measurement.api.v2alpha.DeterministicCountDistinct
import org.wfanet.measurement.api.v2alpha.DeterministicDistribution
import org.wfanet.measurement.api.v2alpha.DifferentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.NoiseMechanism
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.dataprovider.MeasurementResults
import org.wfanet.measurement.dataprovider.RequisitionRefusalException
import org.wfanet.measurement.eventdataprovider.noiser.AbstractNoiser
import org.wfanet.measurement.eventdataprovider.noiser.DirectNoiseMechanism
import org.wfanet.measurement.eventdataprovider.noiser.DpParams
import org.wfanet.measurement.eventdataprovider.noiser.GaussianNoiser
import org.wfanet.measurement.eventdataprovider.noiser.LaplaceNoiser
import java.security.SecureRandom
import java.util.logging.Logger

/**
 * Factory for creating measurement results.
 */
object MeasurementResultFactory {
  private val logger: Logger = Logger.getLogger(this::class.java.name)

  /**
   * Build [Measurement.Result] of the measurement type specified in [MeasurementSpec].
   *
   * @param directProtocolConfig The direct protocol configuration.
   * @param directNoiseMechanism The direct noise mechanism to use.
   * @param measurementSpec The measurement specification.
   * @param sampledVids The sampled VIDs.
   * @param random The random number generator to use.
   * @return The measurement result.
   */
  suspend fun buildMeasurementResult(
    directProtocolConfig: ProtocolConfig.Direct,
    directNoiseMechanism: DirectNoiseMechanism,
    measurementSpec: MeasurementSpec,
    sampledVids: Flow<Long>,
    random: SecureRandom = SecureRandom(),
  ): Measurement.Result {
    val protocolConfigNoiseMechanism = when (directNoiseMechanism) {
      DirectNoiseMechanism.NONE -> {
        NoiseMechanism.NONE
      }
      DirectNoiseMechanism.CONTINUOUS_LAPLACE -> {
        NoiseMechanism.CONTINUOUS_LAPLACE
      }
      else -> {
        NoiseMechanism.CONTINUOUS_GAUSSIAN
      }
    }

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum fields cannot be null.
    return when (measurementSpec.measurementTypeCase) {
      MeasurementSpec.MeasurementTypeCase.REACH_AND_FREQUENCY -> {
        val reachAndFrequencyResultBuilder =  DirectReachAndFrequencyResultBuilder(
            directProtocolConfig,
            sampledVids
            measurementSpec.reachAndFrequency.maximumFrequency,
            measurementSpec.reachAndFrequency.reachPrivacyParams,
            measurementSpec.reachAndFrequency.frequencyPrivacyParams,
            src/main/kotlin/org/wfanet/measurement/edpaggregator/resultsfulfiller/compute/MeasurementResultFactory.kt,
            directNoiseMechanism
            random
        )
        reachAndFrequencyResultBuilder.buildNoisyMeasurementResult()
      }

      MeasurementSpec.MeasurementTypeCase.IMPRESSION -> {
        MeasurementKt.result {
          TODO("Not yet implemented")
        }
      }

      MeasurementSpec.MeasurementTypeCase.DURATION -> {
        MeasurementKt.result {
          TODO("Not yet implemented")
        }
      }

      MeasurementSpec.MeasurementTypeCase.POPULATION -> {
        MeasurementKt.result {
          TODO("Not yet implemented")
        }
      }

      MeasurementSpec.MeasurementTypeCase.REACH -> {
        MeasurementKt.result {
          TODO("Not yet implemented")
        }
      }

      MeasurementSpec.MeasurementTypeCase.MEASUREMENTTYPE_NOT_SET -> {
        error("Measurement type not set.")
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
