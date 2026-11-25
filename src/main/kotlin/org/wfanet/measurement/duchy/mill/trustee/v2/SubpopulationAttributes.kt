/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.mill.trustee.v2

import com.google.protobuf.DescriptorProtos
import com.google.protobuf.Descriptors
import com.google.protobuf.DynamicMessage
import com.google.protobuf.ExtensionRegistry
import com.google.protobuf.TypeRegistry
import org.wfanet.measurement.api.v2alpha.EventAnnotationsProto
import org.wfanet.measurement.api.v2alpha.EventTemplates
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.internal.duchy.PopulationSpec
import org.wfanet.measurement.internal.duchy.protocol.TrusTeeV2

/**
 * The enum-valued population attributes of each subpopulation of a [PopulationSpec].
 *
 * Subpopulations are numbered by their index in [PopulationSpec.getSubpopulationsList]. Attributes
 * are keyed by `<event template name>.<field name>`, e.g. `person.age_group`, and valued by enum
 * symbol. A field left at its default value is not set.
 */
class SubpopulationAttributes private constructor(private val values: List<Map<String, String>>) {
  /** Number of subpopulations. */
  val subpopulationCount: Int
    get() = values.size

  /** Returns the enum symbol at [path] for [subpopulation], or `null` if it does not set it. */
  fun attributeValue(subpopulation: Int, path: String): String? = values[subpopulation][path]

  companion object {
    private val EXTENSION_REGISTRY: ExtensionRegistry =
      ExtensionRegistry.newInstance()
        .also { EventAnnotationsProto.registerAllExtensions(it) }
        .unmodifiable

    /**
     * Resolves the attributes of every subpopulation of [populationSpec], each attribute being a
     * packed event template message of the event message type in [eventDescriptor].
     *
     * @throws IllegalArgumentException if [eventDescriptor] does not describe an event message type
     *   or an attribute is not one of its event templates
     */
    fun build(
      populationSpec: PopulationSpec,
      eventDescriptor: TrusTeeV2.EventDescriptor,
    ): SubpopulationAttributes {
      // Reparse so the event annotations on the descriptors' options read as extensions rather than
      // unknown fields.
      val descriptorSet =
        DescriptorProtos.FileDescriptorSet.parseFrom(
          eventDescriptor.descriptorSet.toByteString(),
          EXTENSION_REGISTRY,
        )
      val eventMessageDescriptor: Descriptors.Descriptor =
        requireNotNull(
          ProtoReflection.buildDescriptors(listOf(descriptorSet)).find {
            it.fullName == eventDescriptor.eventMessageType
          }
        ) {
          "Event message type ${eventDescriptor.eventMessageType} not found in descriptor_set"
        }
      val typeRegistry: TypeRegistry = TypeRegistry.newBuilder().add(eventMessageDescriptor).build()
      val populationFieldsByTemplate: Map<String, List<Descriptors.FieldDescriptor>> =
        EventTemplates.getPopulationFieldsByTemplateType(eventMessageDescriptor).mapKeys {
          it.key.fullName
        }

      val values: List<Map<String, String>> =
        populationSpec.subpopulationsList.map { subpopulation ->
          buildMap {
            for (attribute in subpopulation.attributesList) {
              val templateType: Descriptors.Descriptor =
                requireNotNull(typeRegistry.getDescriptorForTypeUrl(attribute.typeUrl)) {
                  "Attribute type ${attribute.typeUrl} not found in descriptor_set"
                }
              val populationFields: List<Descriptors.FieldDescriptor> =
                requireNotNull(populationFieldsByTemplate[templateType.fullName]) {
                  "${templateType.fullName} is not an event template of " +
                    eventMessageDescriptor.fullName
                }
              val templateName: String = EventTemplates.getTemplateDescriptor(templateType).name
              val message = DynamicMessage.parseFrom(templateType, attribute.value)
              for (field in populationFields) {
                val isSetEnum =
                  field.type == Descriptors.FieldDescriptor.Type.ENUM && message.hasField(field)
                if (!isSetEnum) continue
                val value = message.getField(field) as Descriptors.EnumValueDescriptor
                put("$templateName.${field.name}", value.name)
              }
            }
          }
        }
      return SubpopulationAttributes(values)
    }
  }
}
