/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.api.v2alpha

import com.google.protobuf.Descriptors
import org.wfanet.measurement.api.v2alpha.EventAnnotationsProto
import org.wfanet.measurement.api.v2alpha.EventFieldDescriptor
import org.wfanet.measurement.api.v2alpha.EventTemplateDescriptor
import org.wfanet.measurement.api.v2alpha.MediaType

/** Wrapper around Descriptor for an Event message */
class EventDescriptor(eventDescriptor: Descriptors.Descriptor) {
  data class SupportedReportingFeatures(
    val groupable: Boolean,
    val filterable: Boolean,
    val impressionQualification: Boolean,
  )

  /** Contains info from [EventAnnotationsProto] and [Descriptors.FieldDescriptor] */
  data class EventTemplateFieldInfo(
    val mediaType: MediaType,
    val isPopulationAttribute: Boolean,
    val supportedReportingFeatures: SupportedReportingFeatures,
    val type: Descriptors.FieldDescriptor.Type,
    // Only valid if type is MESSAGE
    val messageTypeFullName: String,
    // Map of Enum name to Enum number
    val enumValuesMap: Map<String, Int>,
  )

  val eventTemplateFieldsMap: Map<String, EventTemplateFieldInfo> =
    buildEventTemplateFieldsMap(eventDescriptor)

  /**
   * Builds Map of EventTemplateField name with respect to Event message to object containing info
   * relevant to Reporting
   *
   * @param eventDescriptor [Descriptors.Descriptor] for Event message
   * @throws IllegalArgumentException when annotation missing or invalid reporting feature
   *   annotation
   */
  private fun buildEventTemplateFieldsMap(
    eventDescriptor: Descriptors.Descriptor
  ): Map<String, EventTemplateFieldInfo> {
    return buildMap {
      for (field in eventDescriptor.fields) {
        if (!field.messageType.options.hasExtension(EventAnnotationsProto.eventTemplate)) {
          throw IllegalArgumentException("EventTemplate missing annotation")
        }

        val templateAnnotation: EventTemplateDescriptor =
          field.messageType.options.getExtension(EventAnnotationsProto.eventTemplate)
        val mediaType = templateAnnotation.mediaType

        for (templateField in field.messageType.fields) {
          if (!templateField.options.hasExtension(EventAnnotationsProto.templateField)) {
            throw IllegalArgumentException("EventTemplate field missing annotation")
          }

          val eventTemplateFieldName = "${templateAnnotation.name}.${templateField.name}"
          val templateFieldAnnotation: EventFieldDescriptor =
            templateField.options.getExtension(EventAnnotationsProto.templateField)

          val isPopulationAttribute = templateFieldAnnotation.populationAttribute
          val supportedReportingFeatures = buildSupportedReportingFeatures(templateFieldAnnotation)
          val enumValuesMap = buildMap {
            if (templateField.type == Descriptors.FieldDescriptor.Type.ENUM) {
              templateField.enumType.values.forEach { put(it.name, it.number) }
            }
          }
          val messageTypeFullName =
            if (templateField.type == Descriptors.FieldDescriptor.Type.MESSAGE) {
              templateField.messageType.fullName
            } else {
              ""
            }

          put(
            eventTemplateFieldName,
            EventTemplateFieldInfo(
              mediaType = mediaType,
              isPopulationAttribute = isPopulationAttribute,
              supportedReportingFeatures = supportedReportingFeatures,
              type = templateField.type,
              messageTypeFullName = messageTypeFullName,
              enumValuesMap = enumValuesMap,
            ),
          )
        }
      }
    }
  }

  /**
   * Builds a [SupportedReportingFeatures] from an [EventFieldDescriptor]
   *
   * @param templateFieldAnnotation annotation for an EventTemplate field
   */
  private fun buildSupportedReportingFeatures(
    templateFieldAnnotation: EventFieldDescriptor
  ): SupportedReportingFeatures {
    val supportedReportingFeaturesMap = buildMap {
      put(EventFieldDescriptor.ReportingFeature.GROUPABLE, false)
      put(EventFieldDescriptor.ReportingFeature.FILTERABLE, false)
      put(EventFieldDescriptor.ReportingFeature.IMPRESSION_QUALIFICATION, false)

      for (reportingFeature in templateFieldAnnotation.reportingFeaturesList) {
        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields cannot be null.
        when (reportingFeature) {
          EventFieldDescriptor.ReportingFeature.GROUPABLE -> {
            put(EventFieldDescriptor.ReportingFeature.GROUPABLE, true)
          }
          EventFieldDescriptor.ReportingFeature.FILTERABLE -> {
            put(EventFieldDescriptor.ReportingFeature.FILTERABLE, true)
          }
          EventFieldDescriptor.ReportingFeature.IMPRESSION_QUALIFICATION -> {
            put(EventFieldDescriptor.ReportingFeature.IMPRESSION_QUALIFICATION, true)
          }
          EventFieldDescriptor.ReportingFeature.REPORTING_FEATURE_UNSPECIFIED,
          EventFieldDescriptor.ReportingFeature.UNRECOGNIZED -> {
            throw IllegalArgumentException("Invalid reporting feature")
          }
        }
      }
    }

    return SupportedReportingFeatures(
      groupable = supportedReportingFeaturesMap[EventFieldDescriptor.ReportingFeature.GROUPABLE]!!,
      filterable =
        supportedReportingFeaturesMap[EventFieldDescriptor.ReportingFeature.FILTERABLE]!!,
      impressionQualification =
        supportedReportingFeaturesMap[
          EventFieldDescriptor.ReportingFeature.IMPRESSION_QUALIFICATION]!!,
    )
  }
}
