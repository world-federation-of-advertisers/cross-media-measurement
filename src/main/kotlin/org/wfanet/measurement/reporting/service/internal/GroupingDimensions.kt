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

package org.wfanet.measurement.reporting.service.internal

import com.google.protobuf.Descriptors
import org.wfanet.measurement.api.v2alpha.EventTemplates
import org.wfanet.measurement.internal.reporting.v2.EventTemplateField
import org.wfanet.measurement.internal.reporting.v2.EventTemplateFieldKt
import org.wfanet.measurement.internal.reporting.v2.eventTemplateField

private typealias Grouping = List<EventTemplateField>

class GroupingDimensions(eventMessageDescriptor: Descriptors.Descriptor) {
  val groupingByFingerprint: Map<Long, Grouping>

  init {
    val maxVersion: Int = EventTemplates.getEventDescriptor(eventMessageDescriptor).currentVersion
    val allGroupableFields: List<EventTemplates.TemplateField> =
      EventTemplates.getGroupableFields(eventMessageDescriptor)
    val valuesByFieldPath: Map<String, List<Descriptors.EnumValueDescriptor>> =
      allGroupableFields.associateBy(EventTemplates.TemplateField::path) { field ->
        // For now, all groupable fields must be enums.
        require(field.descriptor.type == Descriptors.FieldDescriptor.Type.ENUM) {
          "Only enum types are supported for groupable fields"
        }
        field.descriptor.enumType.values
      }

    groupingByFingerprint = buildMap {
      for (version in maxVersion downTo 1) {
        val groupableFields =
          allGroupableFields.filter { field ->
            field.cmmsDescriptor.versionAdded <= version &&
              (field.cmmsDescriptor.versionRemoved == 0 ||
                field.cmmsDescriptor.versionRemoved > version)
          }
        val sets: List<List<Descriptors.EnumValueDescriptor>> =
          groupableFields.map { valuesByFieldPath.getValue(it.path) }
        for (tuple in cartesianProduct(sets)) {
          val grouping = buildList {
            for ((field, value) in groupableFields.zip(tuple)) {
              if (value.number == 0) {
                // Skip the default unspecified value for each enum.
                continue
              }
              add(
                eventTemplateField {
                  path = field.path
                  this.value = EventTemplateFieldKt.fieldValue { enumValue = value.name }
                }
              )
            }
          }
          val existingValue: Grouping? =
            put(Normalization.computeFingerprint(version, grouping), grouping)
          check(existingValue == null) {
            "Fingerprint collision detected between $existingValue and $grouping"
          }
        }
      }
    }
  }

  /**
   * Generates the Cartesian product of the given sets as a sequence of tuples.
   *
   * Adapted from itertools.product. See https://stackoverflow.com/a/62270662
   */
  private fun cartesianProduct(
    sets: List<List<Descriptors.EnumValueDescriptor>>
  ): Sequence<List<Descriptors.EnumValueDescriptor>> = sequence {
    val setCount: Int = sets.size

    val remaining = mutableListOf(1)
    sets.reversed().forEach { remaining.add(0, it.size * remaining[0]) }
    val productCount = remaining.removeAt(0)

    (0 until productCount).forEach { productNumber ->
      val tuple = buildList {
        (0 until setCount).forEach { setIndex ->
          val elementIndex = productNumber / remaining[setIndex] % sets[setIndex].size
          add(sets[setIndex][elementIndex])
        }
      }
      yield(tuple)
    }
  }
}
