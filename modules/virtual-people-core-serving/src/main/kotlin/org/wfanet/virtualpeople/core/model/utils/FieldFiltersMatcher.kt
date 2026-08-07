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

import org.wfanet.virtualpeople.common.FieldFilterProto
import org.wfanet.virtualpeople.common.LabelerEvent
import org.wfanet.virtualpeople.common.LabelerEventOrBuilder
import org.wfanet.virtualpeople.common.fieldfilter.FieldFilter

/** Selects the field filter that a LabelerEvent matches. */
class FieldFiltersMatcher(private val filters: List<FieldFilter>) {

  /**
   * Returns the index of the first matching FieldFilter in [filters]. Returns -1 if no matching.
   */
  fun getFirstMatch(event: LabelerEventOrBuilder): Int {
    return filters.indexOfFirst { it.matches(event) }
  }

  companion object {
    /** build a [FieldFiltersMatcher] from a list of [FieldFilterProto]. */
    fun build(filterConfigs: List<FieldFilterProto>): FieldFiltersMatcher {
      if (filterConfigs.isEmpty()) {
        error("The given FieldFilterProto configs is empty.")
      }
      return FieldFiltersMatcher(
        filterConfigs.map { FieldFilter.create(LabelerEvent.getDescriptor(), it) }
      )
    }
  }
}
