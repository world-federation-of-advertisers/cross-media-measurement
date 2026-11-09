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
import org.wfanet.measurement.api.v2alpha.DeterministicDistribution
import org.wfanet.measurement.api.v2alpha.DifferentialPrivacyParams as CmmsDpParams
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.frequency
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.reach
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.computation.DifferentialPrivacyParams
import org.wfanet.measurement.computation.HistogramComputations
import org.wfanet.measurement.computation.ReachAndFrequencyComputations
import org.wfanet.measurement.computation.ResultMinimumThresholds
import org.wfanet.measurement.computation.ResultNoiser
import org.wfanet.measurement.dataprovider.RequisitionRefusalException
import org.wfanet.measurement.edpaggregator.resultsfulfiller.compute.MeasurementResultBuilder
import org.wfanet.measurement.eventdataprovider.noiser.DirectNoiseMechanism

/**
 * Builder for direct reach and frequency measurement results.
 *
 * @param directProtocolConfig The direct protocol configuration.
 * @param frequencyData the Frequency Histogram.
 * @param maxFrequency The maximum frequency to consider.
 * @param reachPrivacyParams The differential privacy parameters for reach.
 * @param frequencyPrivacyParams The differential privacy parameters for frequency.
 * @param samplingRate The sampling rate used to sample the events.
 * @param directNoiseMechanism The direct noise mechanism to use.
 * @param maxPopulation The max Population that can be returned.
 */
class DirectReachAndFrequencyResultBuilder
private constructor(
  private val directProtocolConfig: ProtocolConfig.Direct,
  private val histogramBuilder: (Int) -> LongArray,
  private val noiserBuilder:
    (DifferentialPrivacyParams, DifferentialPrivacyParams, Int) -> ResultNoiser,
  private val maxFrequency: Int,
  private val reachPrivacyParams: CmmsDpParams,
  private val frequencyPrivacyParams: CmmsDpParams,
  private val samplingRate: Float,
  private val directNoiseMechanism: DirectNoiseMechanism,
  private val maxPopulation: Int?,
  private val resultMinimumThresholds: ResultMinimumThresholds?,
) : MeasurementResultBuilder {
  constructor(
    directProtocolConfig: ProtocolConfig.Direct,
    frequencyData: IntArray,
    maxFrequency: Int,
    reachPrivacyParams: CmmsDpParams,
    frequencyPrivacyParams: CmmsDpParams,
    samplingRate: Float,
    directNoiseMechanism: DirectNoiseMechanism,
    maxPopulation: Int?,
    resultMinimumThresholds: ResultMinimumThresholds?,
  ) : this(
    directProtocolConfig = directProtocolConfig,
    histogramBuilder = { maxFrequency ->
      HistogramComputations.buildHistogram(frequencyData, maxFrequency)
    },
    noiserBuilder = { reachDpParams, frequencyDpParams, maxFrequencyPerUser ->
      buildDirectResultNoiser(
        directNoiseMechanism,
        frequencyData,
        reachDpParams,
        frequencyDpParams,
        maxFrequencyPerUser,
      )
    },
    maxFrequency = maxFrequency,
    reachPrivacyParams = reachPrivacyParams,
    frequencyPrivacyParams = frequencyPrivacyParams,
    samplingRate = samplingRate,
    directNoiseMechanism = directNoiseMechanism,
    maxPopulation = maxPopulation,
    resultMinimumThresholds = resultMinimumThresholds,
  )

  constructor(
    directProtocolConfig: ProtocolConfig.Direct,
    frequencyData: ByteArray,
    maxFrequency: Int,
    reachPrivacyParams: CmmsDpParams,
    frequencyPrivacyParams: CmmsDpParams,
    samplingRate: Float,
    directNoiseMechanism: DirectNoiseMechanism,
    maxPopulation: Int?,
    resultMinimumThresholds: ResultMinimumThresholds?,
  ) : this(
    directProtocolConfig = directProtocolConfig,
    histogramBuilder = { maxFrequency ->
      HistogramComputations.buildHistogram(frequencyData, maxFrequency)
    },
    noiserBuilder = { reachDpParams, frequencyDpParams, maxFrequencyPerUser ->
      buildDirectResultNoiser(
        directNoiseMechanism,
        frequencyData,
        reachDpParams,
        frequencyDpParams,
        maxFrequencyPerUser,
      )
    },
    maxFrequency = maxFrequency,
    reachPrivacyParams = reachPrivacyParams,
    frequencyPrivacyParams = frequencyPrivacyParams,
    samplingRate = samplingRate,
    directNoiseMechanism = directNoiseMechanism,
    maxPopulation = maxPopulation,
    resultMinimumThresholds = resultMinimumThresholds,
  )

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
    val histogram: LongArray = histogramBuilder(maxFrequency)

    val reachValue = getReachValue(histogram)

    val frequencyMap = getFrequencyMap(histogram)

    val protocolConfigNoiseMechanism = directNoiseMechanism.toProtocolConfigNoiseMechanism()

    return MeasurementKt.result {
      reach = reach {
        value = reachValue
        this.noiseMechanism = protocolConfigNoiseMechanism
        deterministicCountDistinct = DeterministicCountDistinct.getDefaultInstance()
      }
      frequency = frequency {
        relativeFrequencyDistribution.putAll(frequencyMap.mapKeys { it.key.toLong() })
        this.noiseMechanism = protocolConfigNoiseMechanism
        deterministicDistribution = DeterministicDistribution.getDefaultInstance()
      }
    }
  }

  private fun getFrequencyMap(histogram: LongArray): Map<Long, Double> {
    if (directNoiseMechanism != DirectNoiseMechanism.NONE) {
      logger.info("Adding $directNoiseMechanism publisher noise to direct reach and frequency...")
    }
    val frequencyDpParams =
      DifferentialPrivacyParams(
        epsilon = frequencyPrivacyParams.epsilon,
        delta = frequencyPrivacyParams.delta,
      )
    return ReachAndFrequencyComputations.computeFrequencyDistribution(
      rawHistogram = histogram,
      maxFrequency = maxFrequency,
      noiser = noiserBuilder(frequencyDpParams, frequencyDpParams, maxFrequency),
      resultMinimumThresholds = resultMinimumThresholds,
      vidSamplingIntervalWidth = samplingRate.toDouble(),
    )
  }

  private fun getReachValue(histogram: LongArray): Long {
    if (directNoiseMechanism != DirectNoiseMechanism.NONE) {
      logger.info("Adding $directNoiseMechanism publisher noise to direct reach...")
    }
    val reachDpParams =
      DifferentialPrivacyParams(
        epsilon = reachPrivacyParams.epsilon,
        delta = reachPrivacyParams.delta,
      )
    return ReachAndFrequencyComputations.computeReach(
      rawHistogram = histogram,
      noiser =
        noiserBuilder(
          reachDpParams,
          reachDpParams,
          resultMinimumThresholds?.reachMaxFrequencyPerUser ?: 1,
        ),
      vidSamplingIntervalWidth = samplingRate.toDouble(),
      vectorSize = maxPopulation,
      resultMinimumThresholds = resultMinimumThresholds,
    )
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
