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
  /**
   * Info for a single template field, keyed by its dotted path in [eventTemplateFieldsByPath].
   *
   * **Media-type inheritance warning (nested-arm paths):** for paths of the form
   * `<template>.<arm>.<field>` (see the private `isOneofArm` helper), [mediaType] is inherited from
   * the parent template. If the parent template carries no `media_type` annotation, [mediaType] is
   * `MEDIA_TYPE_UNSPECIFIED`, which silently makes `IMPRESSION_QUALIFICATION`-tagged children
   * unreachable to all IQF specs -- IQFs require a matching non-unspecified mediaType. Real markets
   * typically want `IMPRESSION_QUALIFICATION` on per-mediaType templates; use `FILTERABLE` /
   * `GROUPABLE` for cross-mediaType arm fields.
   */
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
            if (isOneofArm(templateField)) {
              // MESSAGE-typed field (typically inside a `oneof`) whose message
              // has template_field-annotated fields. Enumerate its nested
              // fields under `<template>.<arm>.<field>`, inheriting the
              // parent template's media type. Handles the market-template
              // shape `Common.oneof edp_specific { Edp1 edp1 = ...; }` where
              // Edp1's own fields carry the annotations.
              for (nestedField in templateField.messageType.fields) {
                validateEventTemplateField(nestedField)
                val nestedFieldAnnotation: EventFieldDescriptor =
                  nestedField.options.getExtension(EventAnnotationsProto.templateField)
                val nestedIsPopulationAttribute = nestedFieldAnnotation.populationAttribute
                val nestedSupportedReportingFeatures =
                  buildSupportedReportingFeatures(nestedFieldAnnotation)
                val nestedEnumType =
                  if (nestedField.type == Descriptors.FieldDescriptor.Type.ENUM) {
                    nestedField.enumType
                  } else {
                    null
                  }
                val nestedPath =
                  "${templateAnnotation.name}.${templateField.name}.${nestedField.name}"
                put(
                  nestedPath,
                  EventTemplateFieldInfo(
                    mediaType = mediaType,
                    isPopulationAttribute = nestedIsPopulationAttribute,
                    supportedReportingFeatures = nestedSupportedReportingFeatures,
                    type = nestedField.type,
                    enumType = nestedEnumType,
                  ),
                )
              }
              continue
            }

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
     * Returns `true` when [field] is a MESSAGE-typed field inside a `oneof` (including `proto3
     * optional`, which is a synthetic oneof-of-one) whose message type contains at least one nested
     * field carrying the `template_field` annotation. Matches the shape real market templates use
     * to expose EDP-specific event fields -- `Common.oneof edp_specific { Edp1 edp1 = ...; }` --
     * where the arm's message holds the template-annotated fields rather than the arm itself.
     * Treated by [buildEventTemplateFieldsByPath] as a nested pseudo-template so its fields become
     * reachable via `<template>.<arm>.<field>` paths and get a `<template>.<arm> != null`
     * null-guard on the generated CEL (an unset oneof arm evaluates to null, unlike a top-level
     * message field whose proto3 default is a zero-instance).
     *
     * Non-oneof MESSAGE-typed fields with template-annotated children are intentionally NOT treated
     * as nested pseudo-templates. If a real deployment ever needs that shape, this predicate can be
     * loosened with a targeted test.
     *
     * **Media-type inheritance warning:** nested-arm fields inherit their parent template's
     * `media_type`. If the parent has no `media_type` annotation (e.g. a demographic-shaped
     * template like `Common`), the inherited value is `MEDIA_TYPE_UNSPECIFIED`, which silently
     * makes any `IMPRESSION_QUALIFICATION`-tagged children unreachable to all IQF specs (IQFs
     * require a matching mediaType). Real markets typically want `IMPRESSION_QUALIFICATION` on
     * per-mediaType templates; use `FILTERABLE` / `GROUPABLE` for cross-mediaType arm fields.
     */
    private fun isOneofArm(field: Descriptors.FieldDescriptor): Boolean {
      if (field.type != Descriptors.FieldDescriptor.Type.MESSAGE) return false
      if (field.containingOneof == null) return false
      // Exempt well-known types the MESSAGE branch already allows as leaf template fields.
      val fullName = field.messageType.fullName
      if (fullName == Duration.getDescriptor().fullName) return false
      if (fullName == Timestamp.getDescriptor().fullName) return false
      return field.messageType.fields.any {
        it.options.hasExtension(EventAnnotationsProto.templateField)
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
