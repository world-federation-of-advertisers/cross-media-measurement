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

/** Utilities for event templates. */
object EventTemplates {
  /**
   * Returns the population fields from [eventMessageDescriptor].
   *
   * Population fields are those which have [EventFieldDescriptor.populationAttribute] set to
   * `true`.
   */
  fun getPopulationFields(
    eventMessageDescriptor: Descriptors.Descriptor
  ): List<Descriptors.FieldDescriptor> = buildList {
    for (field in eventMessageDescriptor.fields) {
      require(field.messageType.options.hasExtension(EventAnnotationsProto.eventTemplate)) {
        "${eventMessageDescriptor.fullName} is not a valid event message type"
      }
      for (templateField in field.messageType.fields) {
        val templateFieldDescriptor: EventFieldDescriptor =
          templateField.options.getExtension(EventAnnotationsProto.templateField)
        if (templateFieldDescriptor.populationAttribute) {
          add(templateField)
        }
      }
    }
  }

  /**
   * Returns the population fields from [eventMessageDescriptor] grouped by event template type.
   *
   * @see getPopulationFields
   */
  fun getPopulationFieldsByTemplateType(
    eventMessageDescriptor: Descriptors.Descriptor
  ): Map<Descriptors.Descriptor, List<Descriptors.FieldDescriptor>> {
    return getPopulationFields(eventMessageDescriptor).groupBy { field -> field.containingType }
  }

  /**
   * Returns the [EventTemplateDescriptor] for the specified [templateType].
   *
   * @throws IllegalArgumentException if [templateType] is not an event template type, i.e. it does
   *   not have an [EventTemplateDescriptor]
   */
  fun getTemplateDescriptor(templateType: Descriptors.Descriptor): EventTemplateDescriptor {
    require(templateType.options.hasExtension(EventAnnotationsProto.eventTemplate)) {
      "${templateType.fullName} is not an event template type"
    }

    return templateType.options.getExtension(EventAnnotationsProto.eventTemplate)
  }

  fun getEventDescriptor(eventMessageDescriptor: Descriptors.Descriptor): EventDescriptor {
    require(eventMessageDescriptor.options.hasExtension(EventAnnotationsProto.event)) {
      "${eventMessageDescriptor.fullName} it not an event message type"
    }

    return eventMessageDescriptor.options.getExtension(EventAnnotationsProto.event)
  }

  /**
   * Returns the groupable fields from [eventMessageDescriptor], ordered by [TemplateField.path].
   *
   * Groupable fields are those which have [EventFieldDescriptor.ReportingFeature.GROUPABLE].
   */
  fun getGroupableFields(eventMessageDescriptor: Descriptors.Descriptor): List<TemplateField> =
    buildList {
      for (eventField: Descriptors.FieldDescriptor in eventMessageDescriptor.fields) {
        require(eventField.messageType.options.hasExtension(EventAnnotationsProto.eventTemplate)) {
          "${eventMessageDescriptor.fullName} is not a valid event message type"
        }
        for (field: Descriptors.FieldDescriptor in eventField.messageType.fields) {
          val cmmsDescriptor: EventFieldDescriptor =
            field.options.getExtension(EventAnnotationsProto.templateField)
          if (
            cmmsDescriptor.reportingFeaturesList.contains(
              EventFieldDescriptor.ReportingFeature.GROUPABLE
            )
          ) {
            add(TemplateField(field, cmmsDescriptor))
          }
        }
      }
      sortBy(TemplateField::path)
    }

  /** Field of an event template. */
  data class TemplateField(
    val descriptor: Descriptors.FieldDescriptor,
    val cmmsDescriptor: EventFieldDescriptor,
  ) {
    val path: String = getTemplateDescriptor(descriptor.containingType).name + "." + descriptor.name
  }
}
