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

package org.wfanet.measurement.reporting.service.internal

import org.wfanet.measurement.internal.reporting.v2.ImpressionQualificationFilter
import org.wfanet.measurement.internal.reporting.v2.ImpressionQualificationFiltersConfig

class ImpressionQualificationFilterMapping(config: ImpressionQualificationFiltersConfig) {
  val impressionQualificationFilters: List<ImpressionQualificationFilter> = buildList {
    for (impressionQualificationFilter in config.impressionQualificationFiltersList) {
      check(
        IMPRESSION_QUALIFICATION_FILTER_RESOURCE_ID_REGEX.matches(
          impressionQualificationFilter.impressionQualificationFilterResourceId
        )
      ) {
        "Invalid external impression qualification filter resource ID ${impressionQualificationFilter.impressionQualificationFilterResourceId}"
      }
      add(impressionQualificationFilter)
    }
  }

  fun getImpressionQualificationFilter(
    impressionQualificationFilterResourceId: String
  ): ImpressionQualificationFilter? {
    for (impressionQualificationFilter in impressionQualificationFilters) {
      if (
        impressionQualificationFilter.impressionQualificationFilterResourceId ==
          impressionQualificationFilterResourceId
      ) {
        return impressionQualificationFilter
      }
    }
    return null
  }

  companion object {
    private val IMPRESSION_QUALIFICATION_FILTER_RESOURCE_ID_REGEX =
      Regex("^[a-zA-Z]([a-zA-Z0-9.-]{0,61}[a-zA-Z0-9])?$")
  }
}
