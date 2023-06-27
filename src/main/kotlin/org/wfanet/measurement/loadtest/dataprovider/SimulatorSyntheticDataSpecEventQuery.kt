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
import com.google.protobuf.kotlin.toByteStringUtf8
import java.util.Locale
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.TimeInterval
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SimulatorSyntheticDataSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec.SubPopulation
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.VidRange

class SimulatorSyntheticDataSpecEventQuery : EventQuery {

  /**
   * Generates a sequence of [DynamicMessage], which represents events.
   *
   * Consumption of [Sequence] throws
   * * [IllegalArgumentException] when [SimulatorSyntheticDataSpec] is invalid, or incompatible
   * * with the [Descriptor].
   */
  fun generateEvents(descriptor: Descriptor, simulatorSyntheticDataSpec: SimulatorSyntheticDataSpec): Sequence<DynamicMessage> {
    val population = simulatorSyntheticDataSpec.population
    val subPopulations = population.subPopulationsList

    return sequence {
      simulatorSyntheticDataSpec.eventGroupSpecList.forEach { syntheticEventGroupSpec ->
        syntheticEventGroupSpec.dateSpecsList.forEach { dateSpec ->
          dateSpec.frequencySpecsList.forEach { frequencySpec ->
            frequencySpec.vidRangeSpecsList.forEach { vidRangeSpec ->
              val subPopulation: SubPopulation =
                vidRangeSpec.vidRange.findSubPopulation(subPopulations)
                  ?: throw IllegalArgumentException()

              val builder = DynamicMessage.newBuilder(descriptor)

              population.populationFieldsList.forEach {
                val subPopulationFieldValue: String? = subPopulation.populationFieldsValuesMap[it]
                if (subPopulationFieldValue != null) {
                  val fieldPath = it.split('.')
                  try {
                    builder.setField(
                      fieldPath,
                      subPopulationFieldValue,
                      descriptor.findFieldByName(fieldPath.first())
                    )
                  } catch (_: NullPointerException) {
                    throw IllegalArgumentException()
                  }
                }
              }

              population.nonPopulationFieldsList.forEach {
                val nonPopulationFieldValue: String? = vidRangeSpec.nonPopulationFieldValuesMap[it]
                if (nonPopulationFieldValue != null) {
                  val fieldPath = it.split('.')
                  try {
                    builder.setField(
                      fieldPath,
                      nonPopulationFieldValue,
                      descriptor.findFieldByName(fieldPath.first())
                    )
                  } catch (_: NullPointerException) {
                    throw IllegalArgumentException()
                  }
                }
              }

              val event: DynamicMessage = builder.build()
              for (i in 1..frequencySpec.frequency) {
                yield(event)
              }
            }
          }
        }
      }
    }
  }

  /**
   * Returns the [SubPopulation] from a list of [SubPopulation] that contains the [VidRange] in its
   * range.
   *
   * Returns null if no [SubPopulation] contains the range.
   */
  private fun VidRange.findSubPopulation(subPopulations: List<SubPopulation>): SubPopulation? {
    val vidRange = this
    subPopulations.forEach {
      val vidSubRange = it.vidSubRange
      if (vidRange.start >= vidSubRange.start && vidRange.endExclusive <= vidSubRange.endExclusive) {
        return it
      }
    }

    return null
  }

  /**
   * Helper function for setting a field value in a [Message.Builder].
   *
   * Throws
   * * [IllegalArgumentException] if field is [FieldDescriptor.Type.MESSAGE].
   * * [NullPointerException] if field can't be found.
   */
  private fun Message.Builder.setField(fieldPath: Collection<String>, value: String, curFieldDescriptor: FieldDescriptor) {
    val builder = this

    if (fieldPath.size == 1) {
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      val fieldValue: Any =
        when (curFieldDescriptor.type) {
          FieldDescriptor.Type.STRING -> value
          FieldDescriptor.Type.BOOL -> value.toBoolean()
          FieldDescriptor.Type.BYTES -> value.toByteStringUtf8()
          FieldDescriptor.Type.ENUM -> curFieldDescriptor.enumType.findValueByName(
            value.uppercase(Locale.getDefault())
          )
          FieldDescriptor.Type.DOUBLE -> value.toDouble()
          FieldDescriptor.Type.FLOAT -> value.toFloat()
          FieldDescriptor.Type.FIXED32,
          FieldDescriptor.Type.INT32,
          FieldDescriptor.Type.SFIXED32,
          FieldDescriptor.Type.SINT32,
          FieldDescriptor.Type.UINT32 -> value.toInt()
          FieldDescriptor.Type.FIXED64,
          FieldDescriptor.Type.INT64,
          FieldDescriptor.Type.SFIXED64,
          FieldDescriptor.Type.SINT64,
          FieldDescriptor.Type.UINT64 -> value.toLong()
          FieldDescriptor.Type.GROUP,
          FieldDescriptor.Type.MESSAGE -> throw IllegalArgumentException()
        }

      if (curFieldDescriptor.isRepeated) {
        builder.addRepeatedField(curFieldDescriptor, fieldValue)
      } else {
        builder.setField(curFieldDescriptor, fieldValue)
      }
      return
    }

    val fieldBuilder = builder.getFieldBuilder(curFieldDescriptor)
    val traversedFieldPath = fieldPath.drop(1)
    fieldBuilder.setField(traversedFieldPath, value, curFieldDescriptor.messageType.findFieldByName(traversedFieldPath.first()))
    val oldMessage = builder.getField(curFieldDescriptor) as Message
    val newMessage =
      fieldBuilder
        .mergeFrom(oldMessage)
        .build()
    builder.setField(curFieldDescriptor, newMessage)
  }

  override fun getUserVirtualIds(
    timeInterval: TimeInterval,
    eventFilter: RequisitionSpec.EventFilter
  ): Sequence<Long> {
    TODO("Not yet implemented")
  }
}
