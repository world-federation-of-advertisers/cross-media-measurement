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
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.computation.DifferentialPrivacyParams
import org.wfanet.measurement.computation.HistogramComputations
import org.wfanet.measurement.computation.NoNoise
import org.wfanet.measurement.computation.ReachAndFrequencyComputations
import org.wfanet.measurement.computation.ResultMinimumThresholds
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
 * @param resultMinimumThresholds Optional small-cell suppression parameters.
 */
class DirectReachResultBuilder(
  private val directProtocolConfig: ProtocolConfig.Direct,
  private val frequencyData: IntArray,
  private val reachPrivacyParams: CmmsDpParams,
  private val samplingRate: Float,
  private val directNoiseMechanism: DirectNoiseMechanism,
  private val maxPopulation: Int?,
  private val resultMinimumThresholds: ResultMinimumThresholds?,
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
        maxFrequency = resultMinimumThresholds?.reachMaxFrequencyPerUser ?: 1,
      )

    val reachValue = getReachValue(histogram)

    val needsThresholdVariance =
      directNoiseMechanism == DirectNoiseMechanism.NONE &&
        resultMinimumThresholds != null &&
        reachValue == 0L &&
        ReachAndFrequencyComputations.computeReach(
          rawHistogram = histogram,
          noiser = NoNoise,
          vidSamplingIntervalWidth = samplingRate.toDouble(),
          vectorSize = maxPopulation,
          resultMinimumThresholds = null,
        ) > 0L
    if (needsThresholdVariance && !directProtocolConfig.hasCustomDirectMethodology()) {
      throw RequisitionRefusalException.Default(
        Requisition.Refusal.Justification.DECLINED,
        "No valid methodology for reporting a thresholded direct result.",
      )
    }

    val protocolConfigNoiseMechanism = directNoiseMechanism.toProtocolConfigNoiseMechanism()

    return MeasurementKt.result {
      reach = reach {
        value = reachValue
        this.noiseMechanism = protocolConfigNoiseMechanism
        if (needsThresholdVariance) {
          customDirectMethodology =
            buildThresholdedReachMethodology(requireNotNull(resultMinimumThresholds))
        } else {
          deterministicCountDistinct = DeterministicCountDistinct.getDefaultInstance()
        }
      }
    }
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
        buildDirectResultNoiser(
          directNoiseMechanism = directNoiseMechanism,
          frequencyData = frequencyData,
          reachDpParams = reachDpParams,
          frequencyDpParams = reachDpParams,
          maxFrequencyPerUser = resultMinimumThresholds?.reachMaxFrequencyPerUser ?: 1,
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
