/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.loadtest.dataprovider

import com.google.protobuf.Descriptors
import com.google.protobuf.DynamicMessage
import org.wfanet.measurement.api.v2alpha.EventTemplates
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.subPopulation
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.vidRange
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.FieldValue
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.pack

/**
 * Converts this synthetic population spec to a [PopulationSpec] without
 * [attributes][PopulationSpec.SubPopulation.attributes].
 */
fun SyntheticPopulationSpec.toPopulationSpecWithoutAttributes(): PopulationSpec {
  return populationSpec {
    subpopulations +=
      subPopulationsList.map { subPopulation ->
        subPopulation {
          vidRanges += vidRange {
            startVid = subPopulation.vidSubRange.start
            endVidInclusive = (subPopulation.vidSubRange.endExclusive - 1)
          }
        }
      }
  }
}

/**
 * Converts this synthetic population spec to a [PopulationSpec].
 *
 * @param eventMessageDescriptor descriptor for the event message type
 * @throws IllegalArgumentException if the type URL of [eventMessageDescriptor] does not match
 *   [SyntheticPopulationSpec.eventMessageTypeUrl]
 * @throws NoSuchElementException if any sub-population is missing an entry for a population field
 * @throws IllegalStateException if any population field is missing a value
 */
fun SyntheticPopulationSpec.toPopulationSpec(
  eventMessageDescriptor: Descriptors.Descriptor
): PopulationSpec {
  require(eventMessageTypeUrl == ProtoReflection.getTypeUrl(eventMessageDescriptor)) {
    "Incorrect event message descriptor for $eventMessageTypeUrl"
  }
  val populationFieldsByTemplateType:
    Map<Descriptors.Descriptor, List<Descriptors.FieldDescriptor>> =
    EventTemplates.getPopulationFieldsByTemplateType(eventMessageDescriptor)

  val source = this
  return populationSpec {
    for (subPopulation in source.subPopulationsList) {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = subPopulation.vidSubRange.start
          endVidInclusive = (subPopulation.vidSubRange.endExclusive - 1)
        }
        for ((templateType, fields) in populationFieldsByTemplateType) {
          val templateName: String = EventTemplates.getTemplateDescriptor(templateType).name
          val templateMessage =
            DynamicMessage.newBuilder(templateType)
              .apply {
                for (field in fields) {
                  val fieldPath = "${templateName}.${field.name}"
                  val value: FieldValue =
                    subPopulation.populationFieldsValuesMap.getValue(fieldPath)
                  when (value.valueCase) {
                    FieldValue.ValueCase.STRING_VALUE -> setField(field, value.stringValue)
                    FieldValue.ValueCase.BOOL_VALUE -> setField(field, value.boolValue)
                    FieldValue.ValueCase.ENUM_VALUE ->
                      setField(field, field.enumType.findValueByNumber(value.enumValue))
                    FieldValue.ValueCase.DOUBLE_VALUE -> setField(field, value.doubleValue)
                    FieldValue.ValueCase.FLOAT_VALUE -> setField(field, value.floatValue)
                    FieldValue.ValueCase.INT32_VALUE -> setField(field, value.int32Value)
                    FieldValue.ValueCase.INT64_VALUE -> setField(field, value.int64Value)
                    FieldValue.ValueCase.DURATION_VALUE -> setField(field, value.durationValue)
                    FieldValue.ValueCase.TIMESTAMP_VALUE -> setField(field, value.timestampValue)
                    FieldValue.ValueCase.VALUE_NOT_SET -> error("Missing value for $fieldPath")
                  }
                }
              }
              .build()

          attributes += templateMessage.pack()
        }
      }
    }
  }
}
