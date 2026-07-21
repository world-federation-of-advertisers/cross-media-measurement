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

package org.wfanet.virtualpeople.core.model.utils

import org.wfanet.virtualpeople.common.LabelerEvent
import org.wfanet.virtualpeople.common.LabelerEventOrBuilder

data class MatrixIndexes(val columnIndex: Int, val rowIndex: Int)

/**
 * Gets the indexes of the selected column and row.
 *
 * Column is selected by applying [hashMatcher] or [filtersMatcher] on the [event]. Row is selected
 * by applying the hashing of the selected column from [rowHashings]. When no column is selected,
 * returns {-1, -1}. [randomSeed] is used as part of the seed when selecting row by hashing. At
 * least one of [hashMatcher] and [filtersMatcher] cannot be null. The output of applying
 * [hashMatcher] or [filtersMatcher] must be in the range of [0, [rowHashings].size() - 1].
 *
 * Accepting `LabelerEventOrBuilder` avoids materializing a full `LabelerEvent` per call when the
 * caller already has the builder; only the hash-matcher path (which needs an immutable event for
 * `FieldMaskUtil.merge`) builds.
 */
fun selectFromMatrix(
  hashMatcher: HashFieldMaskMatcher?,
  filtersMatcher: FieldFiltersMatcher?,
  rowHashings: List<DistributedConsistentHashing>,
  randomSeed: String,
  event: LabelerEventOrBuilder,
): MatrixIndexes {
  val columnIndex =
    when {
      hashMatcher != null ->
        hashMatcher.getMatch(
          when (event) {
            is LabelerEvent -> event
            is LabelerEvent.Builder -> event.build()
            else -> error("Unexpected LabelerEventOrBuilder implementation: ${event::class}")
          }
        )
      filtersMatcher != null -> filtersMatcher.getFirstMatch(event)
      else -> error("No column matcher is set.")
    }
  if (columnIndex == -1) {
    return MatrixIndexes(-1, -1)
  }
  if (columnIndex < 0 || columnIndex >= rowHashings.size) {
    error("The returned index is out of range.")
  }

  /** The seed uses the string representation of actingFingerprint as an unsigned 64-bit integer */
  val rowIndex = rowHashings[columnIndex].hash("$randomSeed${event.actingFingerprint.toULong()}")
  return MatrixIndexes(columnIndex, rowIndex)
}
