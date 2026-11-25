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

import org.wfanet.measurement.computation.ResultMinimumThresholds

/**
 * The first level of small-cell suppression: the noised minimum-reach test of each `DataProvider`
 * slice of a first-pass cell.
 *
 * A cell's slice for a `DataProvider` is the VIDs in the cell that the `DataProvider`'s summand
 * vectors reached. The verdicts are taken once, on the first pass, and are final.
 *
 * @param resultMinimumThresholds the thresholds whose `min_users` each slice is tested against, or
 *   `null` to release every cell from the first pass
 */
class SliceSuppression(
  private val resultMinimumThresholds: ResultMinimumThresholds?,
  private val noise: CellNoise,
) {
  /** Returns the verdict on [cell], a first-pass cell. */
  fun verdict(cell: QueryEvaluator.Cell): Verdict {
    val thresholds: ResultMinimumThresholds = resultMinimumThresholds ?: return Verdict.Release
    val failed: Set<String> =
      cell.summandDataProviders
        .filter { dataProvider ->
          val sliceReach: Long = cell.sliceReach[dataProvider] ?: 0L
          noise.sliceNoiser(cell, dataProvider).noiseReach(sliceReach) < thresholds.minUsers
        }
        .toSet()
    return when {
      failed.isEmpty() -> Verdict.Release
      failed == cell.summandDataProviders -> Verdict.Suppress
      else -> Verdict.Correct(failed)
    }
  }

  /** The outcome of the slice test of one cell. */
  sealed interface Verdict {
    /** Every slice passed: the cell is released from the first pass. */
    data object Release : Verdict

    /** No slice passed: the cell is suppressed. */
    data object Suppress : Verdict

    /**
     * Some slices failed: the cell is rebuilt in a corrective pass that reads the vectors of
     * [failedDataProviders] as zero, and released as rebuilt.
     */
    data class Correct(val failedDataProviders: Set<String>) : Verdict
  }
}
