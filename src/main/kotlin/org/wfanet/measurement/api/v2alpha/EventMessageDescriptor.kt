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

package org.wfanet.measurement.api.v2alpha

import com.google.protobuf.Descriptors
import com.google.protobuf.Duration
import com.google.protobuf.Timestamp

/** Wrapper around Descriptor for an Event message */
class EventMessageDescriptor(eventDescriptor: Descriptors.Descriptor) {
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
    val enumType: Descriptors.EnumDescriptor?,
  )

  /** Map of EventTemplate field path with respect to Event message to info for the field. */
  val eventTemplateFieldsByPath: Map<String, EventTemplateFieldInfo> =
    buildEventTemplateFieldsByPath(eventDescriptor)

  companion object {
    /**
     * Builds Map of EventTemplateField path with respect to Event message to object containing info
     * relevant to Reporting
     *
     * @param eventDescriptor [Descriptors.Descriptor] for Event message
     * @throws IllegalArgumentException when template is missing annotation
     */
    private fun buildEventTemplateFieldsByPath(
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
            validateEventTemplateField(templateField)

            val eventTemplateFieldName = "${templateAnnotation.name}.${templateField.name}"
            val templateFieldAnnotation: EventFieldDescriptor =
              templateField.options.getExtension(EventAnnotationsProto.templateField)

            val isPopulationAttribute = templateFieldAnnotation.populationAttribute
            val supportedReportingFeatures =
              buildSupportedReportingFeatures(templateFieldAnnotation)
            val enumType =
              if (templateField.type == Descriptors.FieldDescriptor.Type.ENUM) {
                templateField.enumType
              } else {
                null
              }

            put(
              eventTemplateFieldName,
              EventTemplateFieldInfo(
                mediaType = mediaType,
                isPopulationAttribute = isPopulationAttribute,
                supportedReportingFeatures = supportedReportingFeatures,
                type = templateField.type,
                enumType = enumType,
              ),
            )
          }
        }
      }
    }

    /**
     * Validates EventTemplate field [Descriptors.FieldDescriptor]
     *
     * @throws IllegalArgumentException when field is missing annotation, or field is invalid
     */
    private fun validateEventTemplateField(eventTemplateField: Descriptors.FieldDescriptor) {
      if (!eventTemplateField.options.hasExtension(EventAnnotationsProto.templateField)) {
        throw IllegalArgumentException("EventTemplate field missing annotation")
      }

      if (eventTemplateField.isRepeated) {
        throw IllegalArgumentException("EventTemplate field cannot be repeated")
      }

      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum fields cannot be null.
      when (eventTemplateField.type) {
        Descriptors.FieldDescriptor.Type.STRING,
        Descriptors.FieldDescriptor.Type.BOOL,
        Descriptors.FieldDescriptor.Type.ENUM,
        Descriptors.FieldDescriptor.Type.DOUBLE,
        Descriptors.FieldDescriptor.Type.FLOAT,
        Descriptors.FieldDescriptor.Type.INT32,
        Descriptors.FieldDescriptor.Type.INT64 -> {}
        Descriptors.FieldDescriptor.Type.MESSAGE ->
          when (val messageName = eventTemplateField.messageType.fullName) {
            Duration.getDescriptor().fullName,
            Timestamp.getDescriptor().fullName -> {}
            else ->
              throw IllegalArgumentException(
                "EventTemplate field has unsupported type $messageName"
              )
          }
        Descriptors.FieldDescriptor.Type.UINT64,
        Descriptors.FieldDescriptor.Type.FIXED64,
        Descriptors.FieldDescriptor.Type.FIXED32,
        Descriptors.FieldDescriptor.Type.GROUP,
        Descriptors.FieldDescriptor.Type.BYTES,
        Descriptors.FieldDescriptor.Type.UINT32,
        Descriptors.FieldDescriptor.Type.SFIXED32,
        Descriptors.FieldDescriptor.Type.SFIXED64,
        Descriptors.FieldDescriptor.Type.SINT32,
        Descriptors.FieldDescriptor.Type.SINT64 ->
          throw IllegalArgumentException(
            "EventTemplate field has unsupported type ${eventTemplateField.type.name.lowercase()}"
          )
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
      var groupable = false
      var filterable = false
      var impressionQualification = false

      for (reportingFeature in templateFieldAnnotation.reportingFeaturesList) {
        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields cannot be null.
        when (reportingFeature) {
          EventFieldDescriptor.ReportingFeature.GROUPABLE -> {
            groupable = true
          }

          EventFieldDescriptor.ReportingFeature.FILTERABLE -> {
            filterable = true
          }

          EventFieldDescriptor.ReportingFeature.IMPRESSION_QUALIFICATION -> {
            impressionQualification = true
          }

          EventFieldDescriptor.ReportingFeature.REPORTING_FEATURE_UNSPECIFIED,
          EventFieldDescriptor.ReportingFeature.UNRECOGNIZED -> {
            throw IllegalArgumentException("Invalid reporting feature")
          }
        }
      }

      return SupportedReportingFeatures(
        groupable = groupable,
        filterable = filterable,
        impressionQualification = impressionQualification,
      )
    }
  }
}
