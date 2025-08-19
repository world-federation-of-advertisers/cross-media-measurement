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

import java.util.logging.Logger
import org.wfanet.measurement.api.v2alpha.DeterministicCountDistinct
import org.wfanet.measurement.api.v2alpha.DifferentialPrivacyParams as CmmsDpParams
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.reach
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.NoiseMechanism
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.computation.DifferentialPrivacyParams
import org.wfanet.measurement.computation.HistogramComputations
import org.wfanet.measurement.computation.KAnonymityParams
import org.wfanet.measurement.computation.ReachAndFrequencyComputations
import org.wfanet.measurement.dataprovider.RequisitionRefusalException
import org.wfanet.measurement.edpaggregator.resultsfulfiller.compute.MeasurementResultBuilder
import org.wfanet.measurement.eventdataprovider.noiser.DirectNoiseMechanism

/**
 * Builder for direct reach measurement results.
 *
 * @param directProtocolConfig The direct protocol configuration.
 * @param frequencyData the Frequency Histogram.
 * @param reachPrivacyParams The differential privacy parameters for reach.
 * @param samplingRate The sampling rate used to sample the events.
 * @param directNoiseMechanism The direct noise mechanism to use.
 * @param maxPopulation The max Population that can be returned. Optional.
 * @param kAnonymityParams The k-anonymity params. Optional.
 */
class DirectReachResultBuilder(
  private val directProtocolConfig: ProtocolConfig.Direct,
  private val frequencyData: IntArray,
  private val reachPrivacyParams: CmmsDpParams,
  private val samplingRate: Float,
  private val directNoiseMechanism: DirectNoiseMechanism,
  private val maxPopulation: Int?,
  private val kAnonymityParams: KAnonymityParams?,
) : MeasurementResultBuilder {

  override suspend fun buildMeasurementResult(): Measurement.Result {
    if (!directProtocolConfig.hasDeterministicCountDistinct()) {
      throw RequisitionRefusalException.Default(
        Requisition.Refusal.Justification.DECLINED,
        "No valid methodologies for direct reach computation.",
      )
    }
    val histogram: LongArray =
      HistogramComputations.buildHistogram(
        frequencyVector = frequencyData,
        maxFrequency = kAnonymityParams?.reachMaxFrequencyPerUser ?: 1,
      )

    val reachValue = getReachValue(histogram)

    val protocolConfigNoiseMechanism =
      when (directNoiseMechanism) {
        DirectNoiseMechanism.NONE -> NoiseMechanism.NONE
        DirectNoiseMechanism.CONTINUOUS_LAPLACE -> NoiseMechanism.CONTINUOUS_LAPLACE
        DirectNoiseMechanism.CONTINUOUS_GAUSSIAN -> NoiseMechanism.CONTINUOUS_GAUSSIAN
      }

    return MeasurementKt.result {
      reach = reach {
        value = reachValue
        this.noiseMechanism = protocolConfigNoiseMechanism
        deterministicCountDistinct = DeterministicCountDistinct.getDefaultInstance()
      }
    }
  }

  private fun getReachValue(histogram: LongArray): Long {
    val reachDpParams =
      if (directNoiseMechanism != DirectNoiseMechanism.NONE) {
        logger.info("Adding $directNoiseMechanism publisher noise to direct reach...")
        require(directNoiseMechanism == DirectNoiseMechanism.CONTINUOUS_GAUSSIAN) {
          "Only Continuous Gaussian is supported for dp noise"
        }
        DifferentialPrivacyParams(
          epsilon = reachPrivacyParams.epsilon,
          delta = reachPrivacyParams.delta,
        )
      } else {
        null
      }
    return ReachAndFrequencyComputations.computeReach(
      rawHistogram = histogram,
      dpParams = reachDpParams,
      vidSamplingIntervalWidth = samplingRate,
      vectorSize = maxPopulation,
      kAnonymityParams = kAnonymityParams,
    )
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
