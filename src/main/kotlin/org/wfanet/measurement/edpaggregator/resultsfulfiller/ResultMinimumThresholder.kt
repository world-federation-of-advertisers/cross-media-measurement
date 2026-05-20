// Copyright 2025 The Cross-Media Measurement Authors
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

import org.wfanet.frequencycount.FrequencyVector
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.computation.HistogramComputations
import org.wfanet.measurement.computation.ReachAndFrequencyComputations
import org.wfanet.measurement.computation.ResultMinimumThresholds
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.FrequencyVectorBuilder

/** Applies minimum result thresholds (small-cell suppression) on frequency vectors. */
object ResultMinimumThresholder {
  /**
   * Applies minimum result thresholds to a frequency vector.
   *
   * Returns an empty FrequencyVector if minimum result thresholds threshold is not met for reach.
   * It does not apply minimum thresholds to individual frequencies that do not meet a threshold.
   *
   * @param measurementSpec The measurement specification
   * @param populationSpec The population specification
   * @param frequencyVectorBuilder The frequency vector builder containing the data
   * @param resultMinimumThresholds The small-cell suppression parameters
   * @param maxPopulation Optional maximum population size
   * @return Either the original frequency vector if minimum result thresholds is met, or an empty
   *   frequency vector
   */
  fun applyThresholds(
    measurementSpec: MeasurementSpec,
    populationSpec: PopulationSpec,
    frequencyVectorBuilder: FrequencyVectorBuilder,
    resultMinimumThresholds: ResultMinimumThresholds,
    maxPopulation: Int?,
  ): FrequencyVector {
    val frequencyData = frequencyVectorBuilder.frequencyDataArray
    val histogram: LongArray =
      HistogramComputations.buildHistogram(
        frequencyVector = frequencyData,
        maxFrequency = resultMinimumThresholds.reachMaxFrequencyPerUser,
      )
    val reachValue =
      ReachAndFrequencyComputations.computeReach(
        rawHistogram = histogram,
        vidSamplingIntervalWidth = measurementSpec.vidSamplingInterval.width.toDouble(),
        vectorSize = maxPopulation,
        dpParams = null,
        resultMinimumThresholds = resultMinimumThresholds,
      )
    return if (reachValue == 0L) {
      // Return an empty frequency vector when minimum result thresholds threshold is not met.
      // Using strict=false to allow empty vector creation without validation errors.
      FrequencyVectorBuilder(
          measurementSpec = measurementSpec,
          populationSpec = populationSpec,
          strict = false,
          overrideImpressionMaxFrequencyPerUser = null,
        )
        .build()
    } else {
      frequencyVectorBuilder.build()
    }
  }
}
