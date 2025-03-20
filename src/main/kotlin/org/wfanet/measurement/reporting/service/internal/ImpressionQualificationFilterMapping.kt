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

import org.wfanet.measurement.common.api.ResourceIds
import org.wfanet.measurement.config.reporting.ImpressionQualificationFilterConfig
import org.wfanet.measurement.internal.reporting.v2.EventTemplateField
import org.wfanet.measurement.internal.reporting.v2.EventTemplateFieldKt
import org.wfanet.measurement.internal.reporting.v2.ImpressionQualificationFilter
import org.wfanet.measurement.internal.reporting.v2.ImpressionQualificationFilterSpec.MediaType
import org.wfanet.measurement.internal.reporting.v2.eventFilter
import org.wfanet.measurement.internal.reporting.v2.eventTemplateField
import org.wfanet.measurement.internal.reporting.v2.impressionQualificationFilter
import org.wfanet.measurement.internal.reporting.v2.impressionQualificationFilterSpec

class ImpressionQualificationFilterMapping(config: ImpressionQualificationFilterConfig) {
  val impressionQualificationFilters:
    List<ImpressionQualificationFilterConfig.ImpressionQualificationFilter> =
    buildList {
        for (impressionQualificationFilter in config.impressionQualificationFiltersList) {
          check(
            EXTERNAL_IMPRESSION_QUALIFICATION_FILTER_ID_REGEX.matches(
              impressionQualificationFilter.externalImpressionQualificationFilterId
            )
          ) {
            "Invalid external impression qualification filter resource ID ${impressionQualificationFilter.externalImpressionQualificationFilterId}"
          }
          add(impressionQualificationFilter)
        }
      }
      .sortedBy { it.externalImpressionQualificationFilterId }

  private val impressionQualificationFilterById:
    Map<Long, ImpressionQualificationFilterConfig.ImpressionQualificationFilter> =
    impressionQualificationFilters.associateBy { it.impressionQualificationFilterId }

  private val impressionQualificationFilterByExternalId:
    Map<String, ImpressionQualificationFilterConfig.ImpressionQualificationFilter> =
    impressionQualificationFilters.associateBy { it.externalImpressionQualificationFilterId }

  init {
    check(impressionQualificationFilterByExternalId.size == impressionQualificationFilters.size) {
      "There are duplicate external ids of impressionQualificationFilters"
    }

    check(impressionQualificationFilterById.size == impressionQualificationFilters.size) {
      "There are duplicate internal ids of impressionQualificationFilters"
    }
  }

  fun getImpressionQualificationFilterById(impressionQualificationFilterInternalId: Long) =
    impressionQualificationFilterById[impressionQualificationFilterInternalId]

  fun getImpressionQualificationByExternalId(externalImpressionQualificationFilterId: String) =
    impressionQualificationFilterByExternalId[externalImpressionQualificationFilterId]

  companion object {
    private val EXTERNAL_IMPRESSION_QUALIFICATION_FILTER_ID_REGEX = ResourceIds.AIP_122_REGEX
  }
}

fun ImpressionQualificationFilterConfig.ImpressionQualificationFilter
  .toImpressionQualificationFilter(): ImpressionQualificationFilter {
  val source = this
  return impressionQualificationFilter {
    externalImpressionQualificationFilterId = source.externalImpressionQualificationFilterId
    filterSpecs +=
      source.filterSpecsList.map { it ->
        impressionQualificationFilterSpec {
          mediaType = it.mediaType.toMediaType()
          filters +=
            it.filtersList.map {
              eventFilter { terms += it.termsList.map { it.toEventTemplateField() } }
            }
        }
      }
  }
}

fun ImpressionQualificationFilterConfig.ImpressionQualificationFilterSpec.MediaType.toMediaType():
  MediaType {
  return when (this) {
    ImpressionQualificationFilterConfig.ImpressionQualificationFilterSpec.MediaType.VIDEO ->
      MediaType.VIDEO
    ImpressionQualificationFilterConfig.ImpressionQualificationFilterSpec.MediaType.DISPLAY ->
      MediaType.DISPLAY
    ImpressionQualificationFilterConfig.ImpressionQualificationFilterSpec.MediaType.OTHER ->
      MediaType.OTHER
    ImpressionQualificationFilterConfig.ImpressionQualificationFilterSpec.MediaType
      .MEDIA_TYPE_UNSPECIFIED -> MediaType.MEDIA_TYPE_UNSPECIFIED
    ImpressionQualificationFilterConfig.ImpressionQualificationFilterSpec.MediaType.UNRECOGNIZED ->
      MediaType.UNRECOGNIZED
  }
}

fun ImpressionQualificationFilterConfig.EventTemplateField.toEventTemplateField():
  EventTemplateField {
  val source = this
  return eventTemplateField {
    path = source.path
    value =
      EventTemplateFieldKt.fieldValue {
        if (source.value.hasBoolValue()) {
          boolValue = source.value.boolValue
        } else if (source.value.hasFloatValue()) {
          floatValue = source.value.floatValue
        } else if (source.value.hasStringValue()) {
          stringValue = source.value.stringValue
        } else if (source.value.hasEnumValue()) {
          enumValue = source.value.enumValue
        }
      }
  }
}
