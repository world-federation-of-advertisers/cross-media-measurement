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

import kotlinx.coroutines.flow.Flow
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
import org.wfanet.measurement.eventdataprovider.noiser.DirectNoiseMechanism
import java.security.SecureRandom
import java.util.logging.Logger
import org.wfanet.measurement.edpaggregator.resultsfulfiller.noise.ReachAndFrequencyNoiseBuilder

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
  override suspend fun buildMeasurementResult(): Measurement.Result {
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
  override suspend fun buildNoisyMeasurementResult(): Measurement.Result {
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
}
