/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import org.wfanet.frequencycount.FrequencyVector
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.computation.HistogramComputations
import org.wfanet.measurement.computation.ImpressionComputations
import org.wfanet.measurement.computation.ReachAndFrequencyComputations
import org.wfanet.measurement.computation.ResultMinimumThresholds
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.FrequencyVectorBuilder

/**
 * Applies minimum result thresholds (small-cell suppression) on frequency vectors at the EDPA
 * level.
 *
 * If the protocol (e.g. TrusTee) already has built-in thresholds that are at least as strict as the
 * EDPA thresholds, the frequency vector is passed through unchanged because the protocol will
 * handle fold-down suppression on the aggregate histogram.
 *
 * If the protocol thresholds are insufficient or absent, the total reach and impressions of the
 * frequency vector are checked against the EDPA thresholds. If they do not meet the thresholds, an
 * empty frequency vector is returned. Otherwise the original frequency vector is returned
 * unchanged.
 *
 * Per-bucket fold-down thresholding is an aggregate-level operation and is handled by the protocol
 * computation, not here.
 */
object ResultMinimumThresholder {
  /**
   * Applies minimum result thresholds to a frequency vector.
   *
   * @param measurementSpec The measurement specification
   * @param populationSpec The population specification
   * @param frequencyVectorBuilder The frequency vector builder containing the data
   * @param resultMinimumThresholds The EDPA small-cell suppression parameters
   * @param maxPopulation Optional maximum population size
   * @param protocolMinUsers The protocol-level minimum users threshold (0 if absent)
   * @param protocolMinImpressions The protocol-level minimum impressions threshold (0 if absent)
   * @return The original frequency vector if thresholds are met or the protocol enforces sufficient
   *   thresholds, or an empty frequency vector if thresholds are not met
   */
  fun applyThresholds(
    measurementSpec: MeasurementSpec,
    populationSpec: PopulationSpec,
    frequencyVectorBuilder: FrequencyVectorBuilder,
    resultMinimumThresholds: ResultMinimumThresholds,
    maxPopulation: Int?,
    protocolMinUsers: Int,
    protocolMinImpressions: Int,
  ): FrequencyVector {
    if (
      protocolMinUsers >= resultMinimumThresholds.minUsers &&
        protocolMinImpressions >= resultMinimumThresholds.minImpressions
    ) {
      return frequencyVectorBuilder.build()
    }

    val frequencyData = frequencyVectorBuilder.frequencyDataArray
    val maxFrequency = resultMinimumThresholds.reachMaxFrequencyPerUser
    val histogram: LongArray =
      HistogramComputations.buildHistogram(
        frequencyVector = frequencyData,
        maxFrequency = maxFrequency,
      )

    val vidSamplingIntervalWidth = measurementSpec.vidSamplingInterval.width.toDouble()
    val scaledReach =
      ReachAndFrequencyComputations.computeReach(
        rawHistogram = histogram,
        vidSamplingIntervalWidth = vidSamplingIntervalWidth,
        vectorSize = maxPopulation,
        dpParams = null,
        resultMinimumThresholds = null,
      )
    val scaledImpressions =
      ImpressionComputations.computeImpressionCount(
        rawHistogram = histogram,
        vidSamplingIntervalWidth = vidSamplingIntervalWidth,
        maxFrequency = null,
        dpParams = null,
        resultMinimumThresholds = null,
      )

    if (
      scaledReach < resultMinimumThresholds.minUsers ||
        scaledImpressions < resultMinimumThresholds.minImpressions
    ) {
      val emptyFrequencyVector =
        FrequencyVectorBuilder(
            measurementSpec = measurementSpec,
            populationSpec = populationSpec,
            strict = false,
            overrideImpressionMaxFrequencyPerUser = null,
          )
          .build()
      return emptyFrequencyVector
    }

    return frequencyVectorBuilder.build()
  }
}
