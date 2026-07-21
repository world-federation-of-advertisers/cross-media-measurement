// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.virtualpeople.core.model

import org.wfanet.virtualpeople.common.LabelerEvent
import org.wfanet.virtualpeople.common.SparseUpdateMatrix
import org.wfanet.virtualpeople.common.fieldfilter.FieldFilter
import org.wfanet.virtualpeople.core.model.utils.*

/**
 * ```
 * A representation of update matrix, which only contains the entries that the
 * probabilities are not zero.
 * Example:
 * The following sparse update matrix
 *     columns {
 *       column_attrs { person_country_code: "COUNTRY_1" }
 *       rows { person_country_code: "UPDATED_COUNTRY_1" }
 *       rows { person_country_code: "UPDATED_COUNTRY_2" }
 *       probabilities: 0.8
 *       probabilities: 0.2
 *     }
 *     columns {
 *       column_attrs { person_country_code: "COUNTRY_2" }
 *       rows { person_country_code: "UPDATED_COUNTRY_1" }
 *       rows { person_country_code: "UPDATED_COUNTRY_2" }
 *       rows { person_country_code: "UPDATED_COUNTRY_3" }
 *       probabilities: 0.2
 *       probabilities: 0.4
 *       probabilities: 0.4
 *     }
 *     columns {
 *       column_attrs { person_country_code: "COUNTRY_3" }
 *       rows { person_country_code: "UPDATED_COUNTRY_3" }
 *       probabilities: 1.0
 *      }
 *      pass_through_non_matches: false
 *     random_seed: "TestSeed"
 * represents the matrix
 *                          "COUNTRY_1"  "COUNTRY_2"  "COUNTRY_3"
 *     "UPDATED_COUNTRY_1"      0.8          0.2            0
 *     "UPDATED_COUNTRY_2"      0.2          0.4            0
 *     "UPDATED_COUNTRY_3"        0          0.4          1.0
 * The column is selected by the matched person_country_code, and the row is
 * selected by probabilities of the selected column.
 * ```
 *
 * @param hashMatcher The matcher used to match input events to the column events when using hash
 *   field mask.
 * @param filtersMatcher The matcher used to match input events to the column conditions when not
 *   using hash field mask.
 * @param rowHashings Each entry of the list represents a hashing based on the probability
 *   distribution of a column. The size of the list is the columns count.
 * @param randomSeed The seed used in hashing.
 * @param rows Each entry of the vector contains all the rows of the corresponding column. The
 *   selected row will be merged to the input event.
 * @param passThroughNonMatches When calling Update, if no column matches, throws error if
 *   passThroughNonMatches is [PassThroughNonMatches.NO].
 */
internal class SparseUpdateMatrixImpl
private constructor(
  private val hashMatcher: HashFieldMaskMatcher?,
  private val filtersMatcher: FieldFiltersMatcher?,
  private val rowHashings: List<DistributedConsistentHashing>,
  private val randomSeed: String,
  private val rows: List<List<LabelerEvent>>,
  private val passThroughNonMatches: PassThroughNonMatches,
) : AttributesUpdaterInterface {

  /**
   * Updates [event] with selected row. The row is selected in 2 steps
   * 1. Select the column with [event] matches the condition.
   * 2. Use hashing to select the row based on the probability distribution of the column.
   *
   * Throws an error if no column matches [event], and [passThroughNonMatches] is
   * [PassThroughNonMatches.NO].
   */
  override fun update(event: LabelerEvent.Builder) {
    val indexes = selectFromMatrix(hashMatcher, filtersMatcher, rowHashings, randomSeed, event)
    if (indexes.columnIndex == -1) {
      if (passThroughNonMatches == PassThroughNonMatches.YES) {
        return
      } else {
        error("No column matching for event: $event")
      }
    }

    if (indexes.rowIndex < 0 || indexes.rowIndex >= rows[indexes.columnIndex].size) {
      error("The returned row index is out of range.")
    }
    event.mergeFrom(rows[indexes.columnIndex][indexes.rowIndex])
    return
  }

  internal companion object {

    /**
     * Always use [AttributesUpdaterInterface].build to get an [AttributesUpdaterInterface] object.
     * Users should not call the factory method or the constructor of the derived classes directly.
     *
     * Throws an error when any of the following happens:
     * 1. [config].columns is empty.
     * 2. [config].columns.column_attrs is not set.
     * 3. [config].columns.rows is empty. In any [config].columns, the counts of probabilities and
     *    rows are not equal.
     * 4. Fails to build [FieldFilter] from any [config].columns.column_attrs. Fails to build
     *    [DistributedConsistentHashing] from the probabilities distribution of any
     *    [config].columns.
     */
    internal fun build(config: SparseUpdateMatrix): SparseUpdateMatrixImpl {
      if (config.columnsCount == 0) {
        error("No column exists in SparseUpdateMatrix: $config")
      }
      config.columnsList.forEach { column ->
        if (!column.hasColumnAttrs()) {
          error("No column_attrs in the column in SparseUpdateMatrix: $column")
        }
        if (column.rowsCount == 0) {
          error("No row exists in the column in SparseUpdateMatrix: $column")
        }
        if (column.rowsCount != column.probabilitiesCount) {
          error(
            "Rows and probabilities are not aligned in the column in SparseUpdateMatrix: $column"
          )
        }
      }

      val hashMatcher: HashFieldMaskMatcher? =
        if (config.hasHashFieldMask()) {
          HashFieldMaskMatcher.build(
            config.columnsList.map { it.columnAttrs },
            config.hashFieldMask,
          )
        } else {
          null
        }
      val filtersMatcher: FieldFiltersMatcher? =
        if (!config.hasHashFieldMask()) {
          FieldFiltersMatcher(config.columnsList.map { FieldFilter.create(it.columnAttrs) })
        } else {
          null
        }

      /** Converts the probability distribution of each column to DistributedConsistentHashing. */
      val rowHashings: List<DistributedConsistentHashing> =
        config.columnsList.map { column ->
          val distributionChoices =
            (0 until column.probabilitiesCount).map {
              DistributionChoice(it, column.getProbabilities(it).toDouble())
            }
          DistributedConsistentHashing(distributionChoices)
        }
      val rows: List<List<LabelerEvent>> = config.columnsList.map { it.rowsList }

      val passThroughNonMatches =
        if (config.passThroughNonMatches) PassThroughNonMatches.YES else PassThroughNonMatches.NO

      return SparseUpdateMatrixImpl(
        hashMatcher,
        filtersMatcher,
        rowHashings,
        config.randomSeed,
        rows,
        passThroughNonMatches,
      )
    }
  }
}
