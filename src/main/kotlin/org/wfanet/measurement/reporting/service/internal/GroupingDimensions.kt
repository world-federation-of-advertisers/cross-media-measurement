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
import org.wfanet.measurement.internal.reporting.v2.EventTemplateFieldKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResult
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResultKt

private typealias EnumTuple = List<Descriptors.EnumValueDescriptor>

class GroupingDimensions(eventMessageDescriptor: Descriptors.Descriptor) {
  val groupingByFingerprint: Map<Long, ReportingSetResult.Dimension.Grouping>

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
        for (tuple: EnumTuple in cartesianProduct(sets)) {
          val grouping =
            ReportingSetResultKt.DimensionKt.grouping {
              for ((field, value) in groupableFields.zip(tuple)) {
                if (value.number == 0) {
                  // Skip the default unspecified value for each enum.
                  continue
                }
                valueByPath[field.path] = EventTemplateFieldKt.fieldValue { enumValue = value.name }
              }
            }
          val existingValue: ReportingSetResult.Dimension.Grouping? =
            put(Normalization.computeFingerprint(version, grouping), grouping)
          check(existingValue == null) {
            "Fingerprint collision detected between $existingValue and $grouping"
          }
        }
      }
    }
  }

  /** Generates the Cartesian product of [sets] as a sequence of tuples. */
  private fun cartesianProduct(
    sets: List<List<Descriptors.EnumValueDescriptor>>
  ): Sequence<EnumTuple> = sequence {
    // Determine how many values to produce for each set, and therefore how many tuples to generate
    // in total.
    val valueCountPerSet = mutableListOf(1)
    for (set in sets.reversed()) {
      valueCountPerSet.add(0, set.size * valueCountPerSet.first())
    }
    val tupleCount = valueCountPerSet.removeFirst()

    val setToValueCount: List<Pair<List<Descriptors.EnumValueDescriptor>, Int>> =
      sets.zip(valueCountPerSet)
    for (tupleIndex in 0 until tupleCount) {
      val tuple = buildList {
        for ((set, valueCount) in setToValueCount) {
          val i = tupleIndex / valueCount % set.size
          add(set[i])
        }
      }
      yield(tuple)
    }
  }
}
