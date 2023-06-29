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
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.TimeInterval
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.FieldValue
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SimulatorSyntheticDataSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec.SubPopulation
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.VidRange

class SimulatorSyntheticDataSpecEventQuery : EventQuery {
  override fun getUserVirtualIds(
    timeInterval: TimeInterval,
    eventFilter: RequisitionSpec.EventFilter
  ): Sequence<Long> {
    TODO("Not yet implemented")
  }

  companion object {

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
        for (syntheticEventGroupSpec: SyntheticEventGroupSpec in simulatorSyntheticDataSpec.eventGroupSpecList) {
          for (dateSpec: SyntheticEventGroupSpec.DateSpec in syntheticEventGroupSpec.dateSpecsList) {
            for (frequencySpec: SyntheticEventGroupSpec.FrequencySpec in dateSpec.frequencySpecsList) {
              for (vidRangeSpec: SyntheticEventGroupSpec.FrequencySpec.VidRangeSpec in frequencySpec.vidRangeSpecsList) {
                val subPopulation: SubPopulation =
                  vidRangeSpec.vidRange.findSubPopulation(subPopulations)
                    ?: throw IllegalArgumentException()

                val builder = DynamicMessage.newBuilder(descriptor)

                population.populationFieldsList.forEach {
                  val subPopulationFieldValue: FieldValue = subPopulation.populationFieldsValuesMap.getValue(it)
                  val fieldPath = it.split('.')
                  val fieldDescriptor: FieldDescriptor = descriptor.findFieldByName(fieldPath.first())
                    ?: throw IllegalArgumentException()
                  builder.setField(
                      fieldPath,
                      subPopulationFieldValue,
                      fieldDescriptor
                    )
                }

                population.nonPopulationFieldsList.forEach {
                  val nonPopulationFieldValue: FieldValue = vidRangeSpec.nonPopulationFieldValuesMap.getValue(it)
                  val fieldPath = it.split('.')
                  val fieldDescriptor: FieldDescriptor = descriptor.findFieldByName(fieldPath.first())
                    ?: throw IllegalArgumentException()
                  builder.setField(
                    fieldPath,
                    nonPopulationFieldValue,
                    fieldDescriptor
                  )
                }

                val event: DynamicMessage = builder.build()
                val numEvents =
                  frequencySpec.frequency *
                    (vidRangeSpec.vidRange.endExclusive - vidRangeSpec.vidRange.start)
                repeat(numEvents.toInt()) {
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
     * Throws
     * * [IllegalArgumentException] if field is [FieldDescriptor.Type.MESSAGE].
     * * [NullPointerException] if field can't be found.
     */
    private fun Message.Builder.setField(
      fieldPath: Collection<String>,
      fieldValue: FieldValue,
      curFieldDescriptor: FieldDescriptor
    ) {
      val builder = this

      if (fieldPath.size == 1) {
        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
        val value: Any =
          when (curFieldDescriptor.type) {
            FieldDescriptor.Type.STRING -> fieldValue.string
            FieldDescriptor.Type.BOOL -> fieldValue.bool
            FieldDescriptor.Type.ENUM ->
              curFieldDescriptor.enumType.findValueByNumber(fieldValue.enum)
            FieldDescriptor.Type.DOUBLE -> fieldValue.double
            FieldDescriptor.Type.FLOAT -> fieldValue.float
            FieldDescriptor.Type.FIXED32,
            FieldDescriptor.Type.INT32,
            FieldDescriptor.Type.SFIXED32,
            FieldDescriptor.Type.SINT32,
            FieldDescriptor.Type.UINT32 -> fieldValue.int32
            FieldDescriptor.Type.FIXED64,
            FieldDescriptor.Type.INT64,
            FieldDescriptor.Type.SFIXED64,
            FieldDescriptor.Type.SINT64,
            FieldDescriptor.Type.UINT64 -> fieldValue.int64
            FieldDescriptor.Type.BYTES,
            FieldDescriptor.Type.GROUP,
            FieldDescriptor.Type.MESSAGE -> throw IllegalArgumentException()
          }

        if (curFieldDescriptor.isRepeated) {
          builder.addRepeatedField(curFieldDescriptor, value)
        } else {
          builder.setField(curFieldDescriptor, value)
        }
        return
      }

      val fieldBuilder = builder.getFieldBuilder(curFieldDescriptor)
      val traversedFieldPath = fieldPath.drop(1)
      val nextFieldDescriptor: FieldDescriptor =  curFieldDescriptor.messageType.findFieldByName(traversedFieldPath.first())
        ?: throw IllegalArgumentException()
      fieldBuilder.setField(
        traversedFieldPath,
        fieldValue,
        nextFieldDescriptor
      )
      val oldMessage = builder.getField(curFieldDescriptor) as Message
      val newMessage = fieldBuilder.mergeFrom(oldMessage).build()
      builder.setField(curFieldDescriptor, newMessage)
    }
  }
}
