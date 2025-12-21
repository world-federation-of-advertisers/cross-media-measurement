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
import org.wfanet.measurement.api.v2alpha.DifferentialPrivacyParams as CmmsDpParams
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.impression
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.NoiseMechanism
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.deterministicCount
import org.wfanet.measurement.computation.DifferentialPrivacyParams
import org.wfanet.measurement.computation.HistogramComputations
import org.wfanet.measurement.computation.ImpressionComputations
import org.wfanet.measurement.computation.KAnonymityParams
import org.wfanet.measurement.dataprovider.RequisitionRefusalException
import org.wfanet.measurement.edpaggregator.resultsfulfiller.compute.MeasurementResultBuilder
import org.wfanet.measurement.eventdataprovider.noiser.DirectNoiseMechanism

/**
 * Builder for direct impression measurement results.
 *
 * @param directProtocolConfig The direct protocol configuration.
 * @param frequencyData the Frequency Histogram.
 * @param privacyParams The differential privacy parameters.
 * @param samplingRate The sampling rate used to sample the events.
 * @param directNoiseMechanism The direct noise mechanism to use.
 * @param maxPopulation The max Population that can be returned.
 * @param maxFrequencyFromSpec The max frequency per user from the measurement spec.
 * @param kAnonymityParams Optional k-anonymity parameters.
 * @param impressionMaxFrequencyPerUser Override for max frequency per user. -1 means no cap.
 * @param totalUncappedImpressions Total impression count without frequency capping.
 */
class DirectImpressionResultBuilder(
  private val directProtocolConfig: ProtocolConfig.Direct,
  private val frequencyData: IntArray,
  private val privacyParams: CmmsDpParams,
  private val samplingRate: Float,
  private val directNoiseMechanism: DirectNoiseMechanism,
  private val maxPopulation: Int?,
  private val maxFrequencyFromSpec: Int,
  private val kAnonymityParams: KAnonymityParams?,
  private val impressionMaxFrequencyPerUser: Int?,
  private val totalUncappedImpressions: Long,
) : MeasurementResultBuilder {

  override suspend fun buildMeasurementResult(): Measurement.Result {
    if (!directProtocolConfig.hasDeterministicCount()) {
      throw RequisitionRefusalException.Default(
        Requisition.Refusal.Justification.DECLINED,
        "No valid methodologies for direct impression computation.",
      )
    }

    val effectiveMaxFrequency =
      impressionMaxFrequencyPerUser?.takeIf { it != -1 } ?: maxFrequencyFromSpec
    val impressionValue = computeImpressionCount(effectiveMaxFrequency)

    val protocolConfigNoiseMechanism =
      when (directNoiseMechanism) {
        DirectNoiseMechanism.NONE -> NoiseMechanism.NONE
        DirectNoiseMechanism.CONTINUOUS_LAPLACE -> NoiseMechanism.CONTINUOUS_LAPLACE
        DirectNoiseMechanism.CONTINUOUS_GAUSSIAN -> NoiseMechanism.CONTINUOUS_GAUSSIAN
      }
    return MeasurementKt.result {
      impression = impression {
        value = impressionValue
        this.noiseMechanism = protocolConfigNoiseMechanism
        this.deterministicCount = deterministicCount {
          customMaximumFrequencyPerUser = effectiveMaxFrequency
        }
      }
    }
  }

  /**
   * Computes the impression count based on frequency data and capping configuration.
   *
   * When [impressionMaxFrequencyPerUser] is -1, uses uncapped impressions directly (with
   * k-anonymity checks if configured). Otherwise, builds a histogram and computes the capped
   * impression count.
   *
   * @param effectiveMaxFrequency The maximum frequency per user to use for capped computations.
   * @return The computed impression count.
   */
  private fun computeImpressionCount(effectiveMaxFrequency: Int): Long {
    // When impressionMaxFrequencyPerUser is -1, use uncapped impressions directly
    val useUncappedImpressions = impressionMaxFrequencyPerUser == -1

    return if (useUncappedImpressions) {
      computeUncappedImpressionValue()
    } else {
      val histogram: LongArray =
        HistogramComputations.buildHistogram(
          frequencyVector = frequencyData,
          maxFrequency = effectiveMaxFrequency,
        )
      getImpressionValue(histogram, effectiveMaxFrequency)
    }
  }

  /**
   * Computes the uncapped impression value, applying k-anonymity checks if configured.
   *
   * @return The uncapped impression count, or 0 if k-anonymity thresholds are not met.
   */
  private fun computeUncappedImpressionValue(): Long {
    if (kAnonymityParams != null) {
      val reachValue = frequencyData.count { it != 0 }
      return if (totalUncappedImpressions < kAnonymityParams.minImpressions) {
        0L
      } else if (reachValue < kAnonymityParams.minUsers) {
        0L
      } else {
        totalUncappedImpressions
      }
    }
    return totalUncappedImpressions
  }

  private fun getImpressionValue(histogram: LongArray, maxFrequency: Int): Long {
    val dpParams =
      if (directNoiseMechanism != DirectNoiseMechanism.NONE) {
        logger.info("Adding $directNoiseMechanism publisher noise to direct impression...")
        require(directNoiseMechanism == DirectNoiseMechanism.CONTINUOUS_GAUSSIAN) {
          "Only Continuous Gaussian is supported for dp noise"
        }
        DifferentialPrivacyParams(epsilon = privacyParams.epsilon, delta = privacyParams.delta)
      } else {
        null
      }
    return ImpressionComputations.computeImpressionCount(
      rawHistogram = histogram,
      dpParams = dpParams,
      vidSamplingIntervalWidth = samplingRate,
      kAnonymityParams = kAnonymityParams,
      maxFrequency = maxFrequency.toLong(),
    )
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
