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
import org.wfanet.measurement.api.v2alpha.CustomDirectMethodologyKt
import org.wfanet.measurement.api.v2alpha.DifferentialPrivacyParams as CmmsDpParams
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.impression
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.customDirectMethodology
import org.wfanet.measurement.api.v2alpha.deterministicCount
import org.wfanet.measurement.computation.DifferentialPrivacyParams
import org.wfanet.measurement.computation.DynamicallyClippedImpressions
import org.wfanet.measurement.computation.HistogramComputations
import org.wfanet.measurement.computation.ImpressionComputations
import org.wfanet.measurement.computation.ResultMinimumThresholds
import org.wfanet.measurement.dataprovider.RequisitionRefusalException
import org.wfanet.measurement.edpaggregator.resultsfulfiller.compute.MeasurementResultBuilder
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams.ImpressionCapMode
import org.wfanet.measurement.eventdataprovider.noiser.DirectNoiseMechanism
import org.wfanet.measurement.eventdataprovider.noiser.DpParams

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
 * @param resultMinimumThresholds Optional small-cell suppression parameters.
 * @param impressionMaxFrequencyPerUser Override for max frequency per user. -1 means no cap.
 * @param totalUncappedImpressions Total impression count without frequency capping.
 * @param impressionCapMode How the per-user cap is chosen.
 */
class DirectImpressionResultBuilder(
  private val directProtocolConfig: ProtocolConfig.Direct,
  private val frequencyData: IntArray,
  private val privacyParams: CmmsDpParams,
  private val samplingRate: Float,
  private val directNoiseMechanism: DirectNoiseMechanism,
  private val maxPopulation: Int?,
  private val maxFrequencyFromSpec: Int,
  private val resultMinimumThresholds: ResultMinimumThresholds?,
  private val impressionMaxFrequencyPerUser: Int?,
  private val totalUncappedImpressions: Long,
  private val impressionCapMode: ImpressionCapMode = ImpressionCapMode.UNSPECIFIED,
) : MeasurementResultBuilder {

  /**
   * Whether the count is released without a per-user cap.
   *
   * [ImpressionCapMode.UNSPECIFIED] reads it off [impressionMaxFrequencyPerUser], which is how
   * configurations written before the explicit modes expressed it.
   */
  private val releaseUncapped: Boolean =
    when (impressionCapMode) {
      ImpressionCapMode.UNCAPPED -> true
      ImpressionCapMode.UNSPECIFIED,
      ImpressionCapMode.UNRECOGNIZED -> impressionMaxFrequencyPerUser == UNCAPPED_SENTINEL
      else -> false
    }

  /** The per-user cap applied when the count is capped. Unused when [releaseUncapped]. */
  private val effectiveMaxFrequency: Int =
    when (impressionCapMode) {
      ImpressionCapMode.USE_MEASUREMENT_SPEC_CAP -> maxFrequencyFromSpec
      ImpressionCapMode.CUSTOM_CAP ->
        requireNotNull(impressionMaxFrequencyPerUser?.takeIf { it > 0 }) {
          "impression_max_frequency_per_user must be greater than zero under CUSTOM_CAP"
        }
      else ->
        impressionMaxFrequencyPerUser?.takeIf { it != UNCAPPED_SENTINEL } ?: maxFrequencyFromSpec
    }

  override suspend fun buildMeasurementResult(): Measurement.Result {
    if (impressionCapMode == ImpressionCapMode.DYNAMIC) {
      return buildDynamicallyClippedResult()
    }

    if (!directProtocolConfig.hasDeterministicCount()) {
      throw RequisitionRefusalException.Default(
        Requisition.Refusal.Justification.DECLINED,
        "No valid methodologies for direct impression computation.",
      )
    }

    val impressionValue = computeImpressionCount(effectiveMaxFrequency)

    val protocolConfigNoiseMechanism = directNoiseMechanism.toProtocolConfigNoiseMechanism()
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
   * Builds a result whose per-user clip is derived from this measurement's frequency distribution.
   *
   * Summing the noised cumulative histogram below the clip is the clipped impression count, so the
   * search and the count come out of one charge with no further draw.
   *
   * The result carries the variance as a custom direct methodology rather than the clip, since the
   * two share a oneof and the reporting server cannot derive this variance: the noise is spread
   * across the bars rather than applied as a single draw.
   *
   * Draws are Gaussian under both mechanisms. The stamp records whether they are seeded, not which
   * distribution they came from.
   */
  private fun buildDynamicallyClippedResult(): Measurement.Result {
    if (directNoiseMechanism !in DYNAMIC_CAP_NOISE_MECHANISMS) {
      throw RequisitionRefusalException.Default(
        Requisition.Refusal.Justification.SPEC_INVALID,
        "Dynamic impression capping requires one of $DYNAMIC_CAP_NOISE_MECHANISMS, " +
          "got $directNoiseMechanism.",
      )
    }

    val clipped: DynamicallyClippedImpressions =
      computeDirectDynamicallyClippedImpressions(
        directNoiseMechanism = directNoiseMechanism,
        frequencyData = frequencyData,
        dpParams = DpParams(privacyParams.epsilon, privacyParams.delta),
        vidSamplingIntervalWidth = samplingRate.toDouble(),
        resultMinimumThresholds = resultMinimumThresholds,
      )
    logger.info("Dynamic impression clip chosen: ${clipped.clip}")

    return MeasurementKt.result {
      impression = impression {
        value = clipped.value
        noiseMechanism = directNoiseMechanism.toProtocolConfigNoiseMechanism()
        customDirectMethodology = customDirectMethodology {
          variance = CustomDirectMethodologyKt.variance { scalar = clipped.variance }
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
    return if (releaseUncapped) {
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
    if (resultMinimumThresholds != null) {
      val reachValue = frequencyData.count { it != 0 }
      return if (totalUncappedImpressions < resultMinimumThresholds.minImpressions) {
        0L
      } else if (reachValue < resultMinimumThresholds.minUsers) {
        0L
      } else {
        totalUncappedImpressions
      }
    }
    return totalUncappedImpressions
  }

  private fun getImpressionValue(histogram: LongArray, maxFrequency: Int): Long {
    if (directNoiseMechanism != DirectNoiseMechanism.NONE) {
      logger.info("Adding $directNoiseMechanism publisher noise to direct impression...")
    }
    val dpParams =
      DifferentialPrivacyParams(epsilon = privacyParams.epsilon, delta = privacyParams.delta)
    return ImpressionComputations.computeImpressionCount(
      rawHistogram = histogram,
      noiser =
        buildDirectResultNoiser(
          directNoiseMechanism = directNoiseMechanism,
          frequencyData = frequencyData,
          reachDpParams = dpParams,
          frequencyDpParams = dpParams,
          maxFrequencyPerUser = maxFrequency,
        ),
      vidSamplingIntervalWidth = samplingRate.toDouble(),
      resultMinimumThresholds = resultMinimumThresholds,
    )
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    /** The `impression_max_frequency_per_user` value meaning "no cap" under UNSPECIFIED. */
    private const val UNCAPPED_SENTINEL = -1

    /**
     * The mechanisms dynamic capping supports. Both noise the cumulative histogram with Gaussian
     * draws; they differ in where the charge comes from and whether the draws are reproducible.
     */
    private val DYNAMIC_CAP_NOISE_MECHANISMS =
      setOf(
        DirectNoiseMechanism.CONTINUOUS_GAUSSIAN,
        DirectNoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE,
      )
  }
}
