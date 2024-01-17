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

import com.google.protobuf.Descriptors.FieldDescriptor
import com.google.protobuf.Message
import java.time.ZoneOffset
import kotlin.random.Random
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.FieldValue
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SimulatorSyntheticDataSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec.FrequencySpec.VidRangeSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec.SubPopulation
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.VidRange
import org.wfanet.measurement.common.LocalDateProgression
import org.wfanet.measurement.common.rangeTo
import org.wfanet.measurement.common.toLocalDate

object SyntheticDataGeneration {
  /**
   * Generates a sequence of [EventQuery.LabeledEvent].
   *
   * Consumption of [Sequence] throws
   * * [IllegalArgumentException] when [SimulatorSyntheticDataSpec] is invalid, or incompatible
   * * with [T].
   *
   * @param messageInstance an instance of the event message type [T]
   * @param populationSpec specification of the synthetic population
   * @param syntheticEventGroupSpec specification of the synthetic event group
   */
  fun <T : Message> generateEvents(
    messageInstance: T,
    populationSpec: SyntheticPopulationSpec,
    syntheticEventGroupSpec: SyntheticEventGroupSpec,
    random: Random = Random(0L),
  ): Sequence<LabeledEvent<T>> {
    val subPopulations = populationSpec.subPopulationsList

    return sequence {
      for (dateSpec: SyntheticEventGroupSpec.DateSpec in syntheticEventGroupSpec.dateSpecsList) {
        val dateProgression = dateSpec.dateRange.toProgression()
        for (frequencySpec: SyntheticEventGroupSpec.FrequencySpec in dateSpec.frequencySpecsList) {

          check(!frequencySpec.hasOverlaps()) { "The VID ranges should be non-overlapping." }

          for (vidRangeSpec: VidRangeSpec in frequencySpec.vidRangeSpecsList) {
            val subPopulation: SubPopulation =
              vidRangeSpec.vidRange.findSubPopulation(subPopulations)
                ?: throw IllegalArgumentException()

            val builder: Message.Builder = messageInstance.newBuilderForType()

            populationSpec.populationFieldsList.forEach {
              val subPopulationFieldValue: FieldValue =
                subPopulation.populationFieldsValuesMap.getValue(it)
              val fieldPath = it.split('.')
              builder.setField(fieldPath, subPopulationFieldValue)
            }

            populationSpec.nonPopulationFieldsList.forEach {
              val nonPopulationFieldValue: FieldValue =
                vidRangeSpec.nonPopulationFieldValuesMap.getValue(it)
              val fieldPath = it.split('.')
              builder.setField(fieldPath, nonPopulationFieldValue)
            }

            @Suppress("UNCHECKED_CAST") // Safe per protobuf API.
            val message = builder.build() as T

            for (date in dateProgression) {
              for (i in 0 until frequencySpec.frequency) {
                val sampledVids = sampleVids(vidRangeSpec, random)
                for (vid in sampledVids) {
                  yield(LabeledEvent(date.atStartOfDay().toInstant(ZoneOffset.UTC), vid, message))
                }
              }
            }
          }
        }
      }
    }
  }

  /**
   * Returns the sampled Vids from [vidRangeSpec]. Given the same [vidRangeSpec] and [randomSeed],
   * returns the same vids. Returns all of the vids if sample size is 0.
   */
  private fun sampleVids(vidRangeSpec: VidRangeSpec, random: Random): Sequence<Long> {
    val vidRangeSequence =
      (vidRangeSpec.vidRange.start until vidRangeSpec.vidRange.endExclusive).asSequence()
    if (vidRangeSpec.sampleSize == 0) {
      return vidRangeSequence
    }

    return vidRangeSequence.shuffled(random).take(vidRangeSpec.sampleSize)
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
   * @throws [IllegalArgumentException] if field is [FieldDescriptor.Type.MESSAGE].
   * @throws [NullPointerException] if field can't be found.
   */
  private fun Message.Builder.setField(fieldPath: Collection<String>, fieldValue: FieldValue) {
    val builder = this
    val field: FieldDescriptor =
      descriptorForType.findFieldByName(fieldPath.first()) ?: throw IllegalArgumentException()

    if (fieldPath.size == 1) {
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      val value: Any =
        when (fieldValue.valueCase) {
          FieldValue.ValueCase.STRING_VALUE -> fieldValue.stringValue
          FieldValue.ValueCase.BOOL_VALUE -> fieldValue.boolValue
          FieldValue.ValueCase.ENUM_VALUE -> field.enumType.findValueByNumber(fieldValue.enumValue)
          FieldValue.ValueCase.DOUBLE_VALUE -> fieldValue.doubleValue
          FieldValue.ValueCase.FLOAT_VALUE -> fieldValue.floatValue
          FieldValue.ValueCase.INT32_VALUE -> fieldValue.int32Value
          FieldValue.ValueCase.INT64_VALUE -> fieldValue.int64Value
          FieldValue.ValueCase.DURATION_VALUE -> fieldValue.durationValue
          FieldValue.ValueCase.TIMESTAMP_VALUE -> fieldValue.timestampValue
          FieldValue.ValueCase.VALUE_NOT_SET -> throw IllegalArgumentException()
        }

      try {
        builder.setField(field, value)
      } catch (e: ClassCastException) {
        throw IllegalArgumentException("Incorrect field value type for $fieldPath", e)
      }
      return
    }

    val nestedBuilder = builder.getFieldBuilder(field)
    val traversedFieldPath = fieldPath.drop(1)
    nestedBuilder.setField(traversedFieldPath, fieldValue)
  }
}

private fun SyntheticEventGroupSpec.DateSpec.DateRange.toProgression(): LocalDateProgression {
  return start.toLocalDate()..endExclusive.toLocalDate().minusDays(1)
}

// Sort the ranges by their start. If there are any consecutive ranges where
// the previous has a larger end than the latter's start, then there is an overlap
private fun SyntheticEventGroupSpec.FrequencySpec.hasOverlaps() =
  vidRangeSpecsList
    .map { it.vidRange }
    .sortedBy { it.start }
    .zipWithNext { first, second -> first.endExclusive > second.start }
    .any { it }
