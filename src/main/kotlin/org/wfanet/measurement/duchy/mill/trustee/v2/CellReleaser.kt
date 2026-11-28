/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.mill.trustee.v2

import org.wfanet.measurement.api.v2alpha.Measurement.Result.MultimeasurementResult.QueryResult
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.MultimeasurementResultKt.QueryResultKt
import org.wfanet.measurement.api.v2alpha.PopulationAttributeValue
import org.wfanet.measurement.api.v2alpha.populationAttributeValue
import org.wfanet.measurement.computation.ReachAndFrequencyComputations
import org.wfanet.measurement.computation.ResultMinimumThresholds
import org.wfanet.measurement.computation.ResultNoiser

/**
 * Noises a cell and thresholds it as a result: the second level of small-cell suppression.
 *
 * The released figures are in-sample: neither reach nor impressions are scaled by the VID sampling
 * interval. The thresholds are compared against the figures scaled by [vidSamplingIntervalWidth],
 * as TrusTEE v1 compares them.
 */
class CellReleaser(
  private val resultMinimumThresholds: ResultMinimumThresholds?,
  private val noise: CellNoise,
  private val vidSamplingIntervalWidth: Double,
) {
  /**
   * Returns the released [cell].
   *
   * Reach, impressions and each frequency bin are noised by [CellNoise.cellNoiser]. If the scaled
   * noised reach is below `min_users` or the scaled noised impressions below `min_impressions`, the
   * cell is suppressed. Otherwise its histogram is folded down by
   * [ReachAndFrequencyComputations.foldDownBelowThresholds].
   */
  fun release(cell: QueryEvaluator.Cell): QueryResult.Cell {
    val noiser: ResultNoiser = noise.cellNoiser(cell)
    val reach: Long = noiser.noiseReach(cell.reach).coerceAtLeast(0L)
    val impressions: Long = noiser.noiseImpressions(cell.impressions).coerceAtLeast(0L)
    val bins =
      LongArray(cell.frequencyHistogram.size) { index ->
        noiser.noiseFrequencyBucket(index, cell.frequencyHistogram[index]).coerceAtLeast(0L)
      }
    val binVariances = DoubleArray(bins.size) { noise.unitVariance }

    val thresholds: ResultMinimumThresholds? = resultMinimumThresholds
    if (thresholds != null) {
      if (
        !thresholds.meets(
          scaledReach = reach / vidSamplingIntervalWidth,
          scaledImpressions = impressions / vidSamplingIntervalWidth,
        )
      ) {
        return suppressed(cell)
      }
      // A folded bin's variance is carried into the next lower bin along with its count.
      ReachAndFrequencyComputations.foldDownBelowThresholds(
        bins,
        thresholds,
        vidSamplingIntervalWidth,
      ) { index ->
        if (index > 0) {
          binVariances[index - 1] += binVariances[index]
        }
        binVariances[index] = 0.0
      }
    }

    return QueryResultKt.cell {
      attributeValues += cell.toAttributeValues()
      this.reach = reach
      reachVariance = noise.unitVariance
      this.impressions = impressions
      impressionVariance = noise.impressionVariance
      frequencyBins += bins.toList()
      frequencyBinVariance += binVariances.toList()
      populationSize = cell.populationSize
    }
  }

  /** Returns [cell] suppressed: zero metrics, no variances, and `suppressed` set. */
  fun suppressed(cell: QueryEvaluator.Cell): QueryResult.Cell {
    return QueryResultKt.cell {
      attributeValues += cell.toAttributeValues()
      reach = 0L
      impressions = 0L
      frequencyBins += List(cell.frequencyHistogram.size) { 0L }
      suppressed = true
      populationSize = cell.populationSize
    }
  }

  /**
   * Returns the grouping values of [this] as public API values. An unset value is an empty
   * [PopulationAttributeValue], keeping the values positional.
   */
  private fun QueryEvaluator.Cell.toAttributeValues(): List<PopulationAttributeValue> =
    attributeValues.map { value ->
      if (value == null) {
        PopulationAttributeValue.getDefaultInstance()
      } else {
        populationAttributeValue { enumValue = value }
      }
    }
}
