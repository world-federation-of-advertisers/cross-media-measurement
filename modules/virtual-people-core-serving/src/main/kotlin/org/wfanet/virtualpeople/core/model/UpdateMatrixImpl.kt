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
import org.wfanet.virtualpeople.common.UpdateMatrix
import org.wfanet.virtualpeople.common.fieldfilter.FieldFilter
import org.wfanet.virtualpeople.core.model.utils.*

/**
 * @param hashMatcher The matcher used to match input events to the column events when using hash
 *   field mask.
 * @param filtersMatcher The matcher used to match input events to the column conditions when not
 *   using hash field mask.
 * @param rowHashings Each entry of the list represents a hashing based on the probability
 *   distribution of a column. The size of the list is the columns count.
 * @param randomSeed The seed used in hashing.
 * @param rows All the rows, of which the selected row will be merged to the input event.
 * @param passThroughNonMatches When calling Update, if no column matches, throws error if
 *   passThroughNonMatches is [PassThroughNonMatches.NO].
 */
internal class UpdateMatrixImpl
private constructor(
  private val hashMatcher: HashFieldMaskMatcher?,
  private val filtersMatcher: FieldFiltersMatcher?,
  private val rowHashings: List<DistributedConsistentHashing>,
  private val randomSeed: String,
  private val rows: List<LabelerEvent>,
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

    if (indexes.rowIndex < 0 || indexes.rowIndex >= rows.size) {
      error("The returned row index is out of range.")
    }
    event.mergeFrom(rows[indexes.rowIndex])
    return
  }

  internal companion object {

    /**
     * Always use [AttributesUpdaterInterface].build to get an [AttributesUpdaterInterface] object.
     * Users should not call the factory method or the constructor of the derived classes directly.
     *
     * Throws an error when any of the following happens:
     * 1. [config].rows is empty.
     * 2. [config].columns is empty.
     * 3. In [config], the probabilities count does not equal to rows count multiplies columns
     *    count.
     * 4. Fails to build [FieldFilter] from any column.
     * 5. Fails to build [DistributedConsistentHashing] from the probability distribution of any
     *    column.
     */
    internal fun build(config: UpdateMatrix): UpdateMatrixImpl {
      if (config.rowsCount == 0) {
        error("No row exists in UpdateMatrix: $config")
      }
      if (config.columnsCount == 0) {
        error("No column exists in UpdateMatrix: $config")
      }
      if (config.rowsCount * config.columnsCount != config.probabilitiesCount) {
        error("Probabilities count must equal to row * column: $config")
      }
      val hashMatcher: HashFieldMaskMatcher? =
        if (config.hasHashFieldMask()) {
          HashFieldMaskMatcher.build(config.columnsList, config.hashFieldMask)
        } else {
          null
        }
      val filtersMatcher: FieldFiltersMatcher? =
        if (!config.hasHashFieldMask()) {
          FieldFiltersMatcher(config.columnsList.map { FieldFilter.create(it) })
        } else {
          null
        }

      /** Converts the probability distribution of each column to [DistributedConsistentHashing]. */
      val rowHashings: List<DistributedConsistentHashing> =
        (0 until config.columnsCount).map { columnIndex ->
          val distributions =
            (0 until config.rowsCount).map { rowIndex ->
              val probability =
                config.getProbabilities(rowIndex * config.columnsCount + columnIndex)
              DistributionChoice(rowIndex, probability.toDouble())
            }
          DistributedConsistentHashing(distributions)
        }

      val passThroughNonMatches =
        if (config.passThroughNonMatches) PassThroughNonMatches.YES else PassThroughNonMatches.NO

      return UpdateMatrixImpl(
        hashMatcher,
        filtersMatcher,
        rowHashings,
        config.randomSeed,
        config.rowsList,
        passThroughNonMatches,
      )
    }
  }
}
