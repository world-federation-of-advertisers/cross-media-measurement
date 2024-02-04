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
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.CartesianSyntheticEventGroupSpecRecipe
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.CartesianSyntheticEventGroupSpecRecipe.FrequencyDimensionSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.CartesianSyntheticEventGroupSpecRecipe.NonPopulationDimensionSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.FieldValue
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpecKt
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec.SubPopulation
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.VidRange
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.syntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.vidRange
import org.wfanet.measurement.common.LocalDateProgression
import org.wfanet.measurement.common.rangeTo
import org.wfanet.measurement.common.toLocalDate

object SyntheticDataGeneration {
  /**
   * Generates a sequence of [EventQuery.LabeledEvent].
   *
   * Consumption of [Sequence] throws
   * * [IllegalArgumentException] when [SyntheticEventGroupSpec] is invalid, or incompatible
   * * with [T].
   *
   * @param messageInstance an instance of the event message type [T]
   * @param populationSpec specification of the synthetic population
   * @param syntheticEventGroupSpec specification of the synthetic event group
   */
  fun <T : Message> generateEvents(
    messageInstance: T,
    populationSpec: SyntheticPopulationSpec,
    syntheticEventGroupSpec: SyntheticEventGroupSpec
  ): Sequence<LabeledEvent<T>> {
    val subPopulations = populationSpec.subPopulationsList

    return sequence {
      for (dateSpec: SyntheticEventGroupSpec.DateSpec in syntheticEventGroupSpec.dateSpecsList) {
        val dateProgression = dateSpec.dateRange.toProgression()
        for (frequencySpec: SyntheticEventGroupSpec.FrequencySpec in dateSpec.frequencySpecsList) {
          for (vidRangeSpec: SyntheticEventGroupSpec.FrequencySpec.VidRangeSpec in
            frequencySpec.vidRangeSpecsList) {
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

            val vids = sampleVids(vidRangeSpec)

            for (vid in vidRangeSpec.vidRange.start until vidRangeSpec.vidRange.endExclusive) {
              for (date in dateProgression) {
                for (i in 0 until frequencySpec.frequency) {
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
   * Returns the [SubPopulation] from a list of [SubPopulation] that contains the [VidRange] in its
   * range.
   *
   * Returns all of the vids if sample size is 0
   */
  private fun sampleVids(
    vidRangeSpec: SyntheticEventGroupSpec.FrequencySpec.VidRangeSpec
  ): Sequence<Long> {
    if (vidRangeSpec.sampleSize == 0L) {
      return vidRangeSpec.vidRange.start until vidRangeSpec.vidRange.endExclusive
    }
    
//     val myList = listOf("Apple", "Banana", "Orange", "Grape")
// val myRandom = Random() // Create your own Random instance
// val shuffledList = myList.shuffled(myRandom) 
    
    return generateSequence { Random.nextInt(1..69) }
        .distinct()
        .take(limit)
        .sorted()
        .toSet()
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

/**
 * Converts a [CartesianSyntheticEventGroupSpecRecipe] to [SyntheticEventGroupSpec] using a
 * [SyntheticPopulationSpec].
 *
 * For each dateSpec in [the CartesianSyntheticEventGroupSpecRecipe];
 * 1. Group the nonPopulationDimensionSpecs based on their field names. These groups specify the
 *    distribution of the data for that dimension.
 * 2. Cross these groups with each other and the frequencyDimensionSpecs. This produces a desired
 *    distribution for all the non population dimensions and the frequencies.
 * 3. Cross this result with the subPopulations defined in the syntheticPopulationSpec. This will
 *    not add any fields since the poopualtion values are defiend in the syntheticPopulationSpec,
 *    this operation is used to query the vid ranges to be used for the resulting cartesian product.
 *
 * Since the algorithm takes the cross product of the population spec, it needs to define at least
 * one sub population.
 *
 * @throws IllegalArgumentException for a [CartesianSyntheticEventGroupSpecRecipe] that implies vids
 *   that are not specified in [syntheticPopulationSpec]
 * @throws IllegalArgumentException for a [syntheticPopulationSpec] that does not have any
 *   subPopulations
 * @throws IllegalArgumentException for a [CartesianSyntheticEventGroupSpecRecipe] that has
 *   frequency or nonpolation dimensions that don't sum up to 1 in thier ratios.
 */
fun CartesianSyntheticEventGroupSpecRecipe.toSyntheticEventGroupSpec(
  syntheticPopulationSpec: SyntheticPopulationSpec
): SyntheticEventGroupSpec {
  check(syntheticPopulationSpec.subPopulationsList.isNotEmpty()) {
    "syntheticPopulationSpec must have sub populations"
  }
  // For each subPopulation keeps track of the vidRange that is avaliable to be used.
  val avaliableSubPopulationRanges =
    syntheticPopulationSpec.subPopulationsList.map { subPopulation ->
      AvaliableSubPopulationRange(subPopulation, subPopulation.vidSubRange.start)
    }

  val mappedDateSpecs =
    this.dateSpecsList.map { dateSpec ->
      val totalReach = dateSpec.totalReach
      // Group NonPopulationDimensionSpecs by name.
      val groupedNonPopulationDimensionSpecs =
        dateSpec.nonPopulationDimensionSpecsList.groupBy { it.fieldName }

      // Check if all non Population dimension ratios sum up to 1.
      groupedNonPopulationDimensionSpecs.forEach { (fieldName, group) ->
        check(group.map { it.ratio }.sum() == 1.0f) {
          "Non poupulation dimension : $fieldName does not sum up to 1."
        }
      }

      // Check if FrequencyDimensionSpec ratios sum up to 1.
      check(dateSpec.frequencyDimensionSpecsList.map { it.ratio }.sum() == 1.0f) {
        "Frequency dimension does not sum up to 1."
      }

      // Take the cartesian product of all non population dimensions
      val nonPopulationCartesianProduct: List<List<NonPopulationDimensionSpec>> =
        groupedNonPopulationDimensionSpecs
          .map { (_, group) -> group }
          .fold(listOf(emptyList<NonPopulationDimensionSpec>())) { acc, inner ->
            acc.flatMap { outer -> inner.map { element -> outer + listOf(element) } }
          }

      val mappedFrequencySpecs = mutableListOf<SyntheticEventGroupSpec.FrequencySpec>()
      for (freqDimension in dateSpec.frequencyDimensionSpecsList) {
        for (nonPopulationSpecs in nonPopulationCartesianProduct) {
          // Number of vids in this region is the product of all the ratios in the dimensions.
          val bucketWidth =
            (totalReach *
                freqDimension.ratio *
                nonPopulationSpecs.map { it.ratio }.reduce { acc, element -> acc * element })
              .toLong()

          avaliableSubPopulationRanges.forEach { avaliableSubPopulationRange ->
            mappedFrequencySpecs +=
              createFrequencySpec(
                bucketWidth,
                freqDimension,
                nonPopulationSpecs,
                avaliableSubPopulationRange
              )
          }
        }
      }
      SyntheticEventGroupSpecKt.dateSpec {
        dateRange = dateSpec.dateRange
        frequencySpecs += mappedFrequencySpecs
      }
    }
  val givenDescription = this.description
  return syntheticEventGroupSpec {
    description = givenDescription
    dateSpecs += mappedDateSpecs
  }
}

private data class AvaliableSubPopulationRange(
  private val subPopulation: SyntheticPopulationSpec.SubPopulation,
  private var avaliableStart: Long
) {
  fun take(amount: Long): VidRange {
    if (avaliableStart + amount >= subPopulation.vidSubRange.endExclusive) {
      throw IllegalArgumentException(
        "CartesianSyntheticEventGroupSpecRecipe implies non existing vids"
      )
    }
    val vidRange = vidRange {
      start = avaliableStart
      endExclusive = avaliableStart + amount
    }
    this.avaliableStart += amount
    return vidRange
  }
}

private fun createFrequencySpec(
  bucketWidth: Long,
  freqDimension: FrequencyDimensionSpec,
  nonPopulationSpecs: List<NonPopulationDimensionSpec>,
  avaliableSubPopulationRange: AvaliableSubPopulationRange
) =
  SyntheticEventGroupSpecKt.frequencySpec {
    frequency = freqDimension.frequency

    vidRangeSpecs +=
      SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
        vidRange = avaliableSubPopulationRange.take(bucketWidth)
        nonPopulationFieldValues.putAll(
          nonPopulationSpecs.associate { it.fieldName to it.fieldValue }
        )
      }
  }
