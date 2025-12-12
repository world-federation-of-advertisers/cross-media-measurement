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

import com.google.protobuf.Descriptors
import org.wfanet.measurement.api.v2alpha.EventMessageDescriptor
import org.wfanet.measurement.api.v2alpha.MediaType as EventAnnotationMediaType
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

class ImpressionQualificationFilterMapping(
  config: ImpressionQualificationFilterConfig,
  val eventMessageDescriptor: Descriptors.Descriptor,
) {
  private val eventDescriptor: EventMessageDescriptor =
    EventMessageDescriptor(eventMessageDescriptor)

  private val eventTemplateFieldsByPath:
    Map<String, EventMessageDescriptor.EventTemplateFieldInfo> =
    eventDescriptor.eventTemplateFieldsByPath

  val impressionQualificationFilters:
    List<ImpressionQualificationFilterConfig.ImpressionQualificationFilter> =
    buildList {
        for (impressionQualificationFilter in config.impressionQualificationFiltersList) {
          if (
            !EXTERNAL_IMPRESSION_QUALIFICATION_FILTER_ID_REGEX.matches(
              impressionQualificationFilter.externalImpressionQualificationFilterId
            )
          ) {
            throw IllegalArgumentException(
              "Invalid external impression qualification filter resource ID ${impressionQualificationFilter.externalImpressionQualificationFilterId}"
            )
          }
          if (impressionQualificationFilter.impressionQualificationFilterId <= 0) {
            throw IllegalArgumentException(
              "Impression qualification filter ID must be positive. Got: ${impressionQualificationFilter.impressionQualificationFilterId}"
            )
          }
          val mediaTypesEncountered =
            mutableSetOf<
              ImpressionQualificationFilterConfig.ImpressionQualificationFilterSpec.MediaType
            >()
          for (impressionQualificationFilterSpec in impressionQualificationFilter.filterSpecsList) {
            if (mediaTypesEncountered.contains(impressionQualificationFilterSpec.mediaType)) {
              throw IllegalArgumentException(
                "Duplicate MediaType in $impressionQualificationFilter"
              )
            } else {
              mediaTypesEncountered.add(impressionQualificationFilterSpec.mediaType)
            }

            if (!isImpressionQualificationFilterSpecValid(impressionQualificationFilterSpec)) {
              throw IllegalArgumentException(
                "Invalid impression qualification filter spec: $impressionQualificationFilterSpec"
              )
            }
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
    if (impressionQualificationFilterByExternalId.size < impressionQualificationFilters.size) {
      throw IllegalArgumentException(
        "There are duplicate external ids of impressionQualificationFilters"
      )
    }

    if (impressionQualificationFilterById.size < impressionQualificationFilters.size) {
      throw IllegalArgumentException(
        "There are duplicate internal ids of impressionQualificationFilters"
      )
    }
  }

  fun getImpressionQualificationFilterById(impressionQualificationFilterInternalId: Long) =
    impressionQualificationFilterById[impressionQualificationFilterInternalId]

  fun getImpressionQualificationByExternalId(externalImpressionQualificationFilterId: String) =
    impressionQualificationFilterByExternalId[externalImpressionQualificationFilterId]

  private fun ImpressionQualificationFilterConfig.ImpressionQualificationFilterSpec.MediaType
    .toEventAnnotationMediaType(): EventAnnotationMediaType {
    return when (this) {
      ImpressionQualificationFilterConfig.ImpressionQualificationFilterSpec.MediaType.VIDEO ->
        EventAnnotationMediaType.VIDEO
      ImpressionQualificationFilterConfig.ImpressionQualificationFilterSpec.MediaType.DISPLAY ->
        EventAnnotationMediaType.DISPLAY
      ImpressionQualificationFilterConfig.ImpressionQualificationFilterSpec.MediaType.OTHER ->
        EventAnnotationMediaType.OTHER
      ImpressionQualificationFilterConfig.ImpressionQualificationFilterSpec.MediaType
        .MEDIA_TYPE_UNSPECIFIED,
      ImpressionQualificationFilterConfig.ImpressionQualificationFilterSpec.MediaType
        .UNRECOGNIZED -> throw UnsupportedOperationException()
    }
  }

  private fun isImpressionQualificationFilterSpecValid(
    impressionQualificationFilterSpec:
      ImpressionQualificationFilterConfig.ImpressionQualificationFilterSpec
  ): Boolean {
    if (impressionQualificationFilterSpec.filtersList.isEmpty()) {
      return false
    }

    if (
      impressionQualificationFilterSpec.mediaType ==
        ImpressionQualificationFilterConfig.ImpressionQualificationFilterSpec.MediaType
          .MEDIA_TYPE_UNSPECIFIED
    ) {
      return false
    }

    for (eventFilter in impressionQualificationFilterSpec.filtersList) {
      for (eventTemplateField in eventFilter.termsList) {
        if (eventTemplateField.path.isEmpty()) {
          return false
        }

        val eventTemplateFieldInfo =
          eventTemplateFieldsByPath[eventTemplateField.path] ?: return false

        if (!eventTemplateFieldInfo.supportedReportingFeatures.impressionQualification) {
          return false
        }

        if (
          eventTemplateFieldInfo.mediaType !=
            impressionQualificationFilterSpec.mediaType.toEventAnnotationMediaType()
        ) {
          return false
        }

        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum fields cannot be null.
        when (eventTemplateField.value.selectorCase) {
          ImpressionQualificationFilterConfig.EventTemplateField.FieldValue.SelectorCase
            .STRING_VALUE -> {
            if (eventTemplateFieldInfo.type != Descriptors.FieldDescriptor.Type.STRING) {
              return false
            }
          }
          ImpressionQualificationFilterConfig.EventTemplateField.FieldValue.SelectorCase
            .ENUM_VALUE -> {
            if (
              eventTemplateFieldInfo.type != Descriptors.FieldDescriptor.Type.ENUM ||
                eventTemplateFieldInfo.enumType?.findValueByName(
                  eventTemplateField.value.enumValue
                ) == null
            ) {
              return false
            }
          }
          ImpressionQualificationFilterConfig.EventTemplateField.FieldValue.SelectorCase
            .BOOL_VALUE ->
            if (eventTemplateFieldInfo.type != Descriptors.FieldDescriptor.Type.BOOL) {
              return false
            }
          ImpressionQualificationFilterConfig.EventTemplateField.FieldValue.SelectorCase
            .FLOAT_VALUE -> {
            if (eventTemplateFieldInfo.type != Descriptors.FieldDescriptor.Type.FLOAT) {
              return false
            }
          }
          ImpressionQualificationFilterConfig.EventTemplateField.FieldValue.SelectorCase
            .SELECTOR_NOT_SET -> {
            return false
          }
        }
      }
    }

    return true
  }

  companion object {
    private val EXTERNAL_IMPRESSION_QUALIFICATION_FILTER_ID_REGEX = ResourceIds.AIP_122_REGEX

    private fun ImpressionQualificationFilterConfig.ImpressionQualificationFilterSpec.MediaType
      .toMediaType(): MediaType {
      return when (this) {
        ImpressionQualificationFilterConfig.ImpressionQualificationFilterSpec.MediaType.VIDEO ->
          MediaType.VIDEO
        ImpressionQualificationFilterConfig.ImpressionQualificationFilterSpec.MediaType.DISPLAY ->
          MediaType.DISPLAY
        ImpressionQualificationFilterConfig.ImpressionQualificationFilterSpec.MediaType.OTHER ->
          MediaType.OTHER
        ImpressionQualificationFilterConfig.ImpressionQualificationFilterSpec.MediaType
          .MEDIA_TYPE_UNSPECIFIED -> MediaType.MEDIA_TYPE_UNSPECIFIED
        ImpressionQualificationFilterConfig.ImpressionQualificationFilterSpec.MediaType
          .UNRECOGNIZED -> MediaType.UNRECOGNIZED
      }
    }

    private fun ImpressionQualificationFilterConfig.EventTemplateField.toEventTemplateField():
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

    fun ImpressionQualificationFilterConfig.ImpressionQualificationFilter
      .toImpressionQualificationFilter(): ImpressionQualificationFilter {
      val source = this
      val impressionQualificationFilter = impressionQualificationFilter {
        externalImpressionQualificationFilterId = source.externalImpressionQualificationFilterId
        filterSpecs +=
          source.filterSpecsList.map { filterSpec ->
            impressionQualificationFilterSpec {
              mediaType = filterSpec.mediaType.toMediaType()
              filters +=
                filterSpec.filtersList.map { eventFilter ->
                  eventFilter { terms += eventFilter.termsList.map { it.toEventTemplateField() } }
                }
            }
          }
      }
      return impressionQualificationFilter
    }
  }
}
