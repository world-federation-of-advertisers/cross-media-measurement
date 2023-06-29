/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

import com.google.protobuf.Descriptors.Descriptor
import com.google.protobuf.Descriptors.FieldDescriptor
import com.google.protobuf.DynamicMessage
import com.google.protobuf.Message
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.FieldValue
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SimulatorSyntheticDataSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec.SubPopulation
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.VidRange

object SyntheticDataGeneration {
  /**
   * Generates a sequence of [DynamicMessage], which represents events.
   *
   * Consumption of [Sequence] throws
   * * [IllegalArgumentException] when [SimulatorSyntheticDataSpec] is invalid, or incompatible
   * * with the [Descriptor].
   */
  @JvmStatic
  fun generateEvents(
    descriptor: Descriptor,
    simulatorSyntheticDataSpec: SimulatorSyntheticDataSpec
  ): Sequence<DynamicMessage> {
    val population = simulatorSyntheticDataSpec.population
    val subPopulations = population.subPopulationsList

    return sequence {
      for (syntheticEventGroupSpec: SyntheticEventGroupSpec in
        simulatorSyntheticDataSpec.eventGroupSpecList) {
        for (dateSpec: SyntheticEventGroupSpec.DateSpec in
          syntheticEventGroupSpec.dateSpecsList) {
          for (frequencySpec: SyntheticEventGroupSpec.FrequencySpec in
            dateSpec.frequencySpecsList) {
            for (vidRangeSpec: SyntheticEventGroupSpec.FrequencySpec.VidRangeSpec in
              frequencySpec.vidRangeSpecsList) {
              val subPopulation: SubPopulation =
                vidRangeSpec.vidRange.findSubPopulation(subPopulations)
                  ?: throw IllegalArgumentException()

              val builder = DynamicMessage.newBuilder(descriptor)

              population.populationFieldsList.forEach {
                val subPopulationFieldValue: FieldValue =
                  subPopulation.populationFieldsValuesMap.getValue(it)
                val fieldPath = it.split('.')
                builder.setField(fieldPath, subPopulationFieldValue)
              }

              population.nonPopulationFieldsList.forEach {
                val nonPopulationFieldValue: FieldValue =
                  vidRangeSpec.nonPopulationFieldValuesMap.getValue(it)
                val fieldPath = it.split('.')
                builder.setField(fieldPath, nonPopulationFieldValue)
              }

              val event: DynamicMessage = builder.build()
              val numEvents =
                frequencySpec.frequency *
                  (vidRangeSpec.vidRange.endExclusive - vidRangeSpec.vidRange.start)
              repeat(numEvents.toInt()) { yield(event) }
            }
          }
        }
      }
    }
  }

  /**
   * Returns the [SubPopulation] from a list of [SubPopulation] that contains the [VidRange] in
   * its range.
   *
   * Returns null if no [SubPopulation] contains the range.
   */
  private fun VidRange.findSubPopulation(subPopulations: List<SubPopulation>): SubPopulation? {
    val vidRange = this
    subPopulations.forEach {
      val vidSubRange = it.vidSubRange
      if (
        vidRange.start >= vidSubRange.start && vidRange.endExclusive <= vidSubRange.endExclusive
      ) {
        return it
      }
    }

    return null
  }

  /**
   * Helper function for setting a field value in a [Message.Builder].
   *
   * @throws
   * * [IllegalArgumentException] if field is [FieldDescriptor.Type.MESSAGE].
   * * [NullPointerException] if field can't be found.
   */
  private fun Message.Builder.setField(
    fieldPath: Collection<String>,
    fieldValue: FieldValue
  ) {
    val builder = this
    val curFieldDescriptor: FieldDescriptor =
      descriptorForType.findFieldByName(fieldPath.first())
        ?: throw IllegalArgumentException()

    if (fieldPath.size == 1) {
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      val value: Any =
        when (fieldValue.valueCase) {
          FieldValue.ValueCase.STRING_VALUE -> fieldValue.stringValue
          FieldValue.ValueCase.BOOL_VALUE -> fieldValue.boolValue
          FieldValue.ValueCase.ENUM_VALUE ->
            curFieldDescriptor.enumType.findValueByNumber(fieldValue.enumValue)
          FieldValue.ValueCase.DOUBLE_VALUE -> fieldValue.doubleValue
          FieldValue.ValueCase.FLOAT_VALUE -> fieldValue.floatValue
          FieldValue.ValueCase.INT32_VALUE -> fieldValue.int32Value
          FieldValue.ValueCase.INT64_VALUE -> fieldValue.int64Value
          FieldValue.ValueCase.VALUE_NOT_SET -> throw IllegalArgumentException()
        }

      builder.setField(curFieldDescriptor, value)

      return
    }

    val nestedBuilder = builder.getFieldBuilder(curFieldDescriptor)
    val traversedFieldPath = fieldPath.drop(1)
    nestedBuilder.setField(traversedFieldPath, fieldValue)
    builder.setField(curFieldDescriptor, nestedBuilder.build())
  }
}
