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
import org.wfanet.measurement.computation.ResultMinimumThresholds
import org.wfanet.measurement.computation.ResultNoiser

/**
 * Noises a cell and thresholds it as a result: the second level of small-cell suppression.
 *
 * The figures are in-sample: neither reach nor impressions are scaled by the VID sampling interval,
 * and the thresholds are compared against the in-sample figures.
 */
class CellReleaser(
  private val resultMinimumThresholds: ResultMinimumThresholds?,
  private val noise: CellNoise,
) {
  /**
   * Returns the released [cell].
   *
   * Reach, impressions and each frequency bin are noised by [CellNoise.cellNoiser]. If the noised
   * reach is below `min_users` or the noised impressions below `min_impressions`, the cell is
   * suppressed. Otherwise its histogram is folded down from the top bin: a bin whose noised count
   * is below `min_users`, or whose frequency times count is below `min_impressions`, is zeroed and
   * its count added to the next lower bin, which is then tested in turn.
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
      if (reach < thresholds.minUsers || impressions < thresholds.minImpressions) {
        return suppressed(cell)
      }
      foldDown(bins, binVariances, thresholds)
    }

    return QueryResultKt.cell {
      attributeValues += cell.toAttributeValues()
      this.reach = reach
      reachVariance = noise.unitVariance
      this.impressions = impressions
      impressionVariance = noise.impressionVariance
      frequencyBins += bins.toList()
      frequencyBinVariance += binVariances.toList()
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
    }
  }

  /**
   * Folds [bins] down from the top bin, carrying [variances] along with the counts.
   *
   * This is the fold of `ReachAndFrequencyComputations.computeFrequencyDistribution`, which returns
   * rates rather than counts and scales by the sampling interval, so it is restated here over
   * in-sample counts.
   */
  private fun foldDown(
    bins: LongArray,
    variances: DoubleArray,
    thresholds: ResultMinimumThresholds,
  ) {
    for (index in bins.indices.reversed()) {
      val frequency = index + 1L
      val count = bins[index]
      if (count < thresholds.minUsers || frequency * count < thresholds.minImpressions) {
        bins[index] = 0L
        if (index > 0) {
          bins[index - 1] += count
          variances[index - 1] += variances[index]
        }
        variances[index] = 0.0
      }
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
