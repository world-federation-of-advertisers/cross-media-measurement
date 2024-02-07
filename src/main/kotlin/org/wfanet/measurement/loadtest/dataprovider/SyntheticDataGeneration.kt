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

import com.google.common.hash.Hashing
import com.google.protobuf.Descriptors.FieldDescriptor
import com.google.protobuf.Message
import com.google.protobuf.kotlin.toByteStringUtf8
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.time.ZoneOffset
import kotlin.math.max
import kotlin.math.min
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.CartesianSyntheticEventGroupSpecRecipe
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.FieldValue
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SimulatorSyntheticDataSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec.FrequencySpec.VidRangeSpec
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
  private val VID_SAMPLING_FINGERPRINT_FUNCTION = Hashing.farmHashFingerprint64()
  private const val FINGERPRINT_BUFFER_SIZE_BYTES = 512

  /**
   * Generates a sequence of [LabeledEvent].
   *
   * Consumption of [Sequence] throws
   * * [IllegalStateException] when [SimulatorSyntheticDataSpec] is invalid, or incompatible
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
                ?: error("Sub-population not found")
            check(vidRangeSpec.samplingRate in 0.0..1.0) { "Invalid sampling_rate" }
            if (vidRangeSpec.sampled) {
              check(syntheticEventGroupSpec.samplingNonce != 0L) {
                "sampling_nonce is required for VID sampling"
              }
            }

            val builder: Message.Builder = messageInstance.newBuilderForType()

            populationSpec.populationFieldsList.forEach {
              val subPopulationFieldValue: FieldValue =
                subPopulation.populationFieldsValuesMap.getValue(it)
              val fieldPath = it.split('.')
              try {
                builder.setField(fieldPath, subPopulationFieldValue)
              } catch (e: IllegalArgumentException) {
                throw IllegalStateException(e)
              }
            }

            populationSpec.nonPopulationFieldsList.forEach {
              val nonPopulationFieldValue: FieldValue =
                vidRangeSpec.nonPopulationFieldValuesMap.getValue(it)
              val fieldPath = it.split('.')
              try {
                builder.setField(fieldPath, nonPopulationFieldValue)
              } catch (e: IllegalArgumentException) {
                throw IllegalStateException(e)
              }
            }

            @Suppress("UNCHECKED_CAST") // Safe per protobuf API.
            val message = builder.build() as T

            for (date in dateProgression) {
              for (i in 1..frequencySpec.frequency) {
                for (vid in vidRangeSpec.sampledVids(syntheticEventGroupSpec.samplingNonce)) {
                  yield(LabeledEvent(date.atStartOfDay().toInstant(ZoneOffset.UTC), vid, message))
                }
              }
            }
          }
        }
      }
    }
  }

  /** Returns the VIDs which are in the sample for this [VidRangeSpec]. */
  private fun VidRangeSpec.sampledVids(samplingNonce: Long): Sequence<Long> {
    return (vidRange.start until vidRange.endExclusive).asSequence().filter {
      inSample(it, samplingNonce)
    }
  }

  /** Returns whether [vid] is in the sample specified by this [VidRangeSpec]. */
  private fun VidRangeSpec.inSample(vid: Long, samplingNonce: Long): Boolean {
    if (!sampled) {
      return true
    }

    val buffer =
      ByteBuffer.allocate(FINGERPRINT_BUFFER_SIZE_BYTES)
        .order(ByteOrder.LITTLE_ENDIAN)
        .putLong(vid)
        .putLong(samplingNonce)
        .putFieldValueMap(nonPopulationFieldValuesMap)
        .flip()
    val fingerprint = VID_SAMPLING_FINGERPRINT_FUNCTION.hashBytes(buffer).asLong()
    val rangeValue: Double = fingerprint.toDouble() / Long.MAX_VALUE
    return rangeValue in -samplingRate..samplingRate
  }

  /** Whether the [VidRange] in this [VidRangeSpec] should be sampled. */
  private val VidRangeSpec.sampled: Boolean
    get() = samplingRate > 0.0 && samplingRate < 1.0

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
// the previous has a larger end than the latter's start, then there is an overlap.
private fun SyntheticEventGroupSpec.FrequencySpec.hasOverlaps() =
  vidRangeSpecsList
    .map { it.vidRange }
    .sortedBy { it.start }
    .zipWithNext()
    .any { (first, second) -> first.overlaps(second) }

private fun VidRange.overlaps(other: VidRange) =
  max(start, other.start) < min(endExclusive, other.endExclusive)

private fun ByteBuffer.putFieldValueMap(fieldValueMap: Map<String, FieldValue>): ByteBuffer {
  for ((key, value) in fieldValueMap.entries.sortedBy { it.key }) {
    putStringUtf8(key)
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum accessors cannot return null.
    when (value.valueCase) {
      FieldValue.ValueCase.STRING_VALUE -> putStringUtf8(value.stringValue)
      FieldValue.ValueCase.BOOL_VALUE -> putBoolean(value.boolValue)
      FieldValue.ValueCase.DOUBLE_VALUE -> putDouble(value.doubleValue)
      FieldValue.ValueCase.FLOAT_VALUE -> putFloat(value.floatValue)
      FieldValue.ValueCase.ENUM_VALUE,
      FieldValue.ValueCase.INT32_VALUE -> putInt(value.enumValue)
      FieldValue.ValueCase.INT64_VALUE -> putLong(value.int64Value)
      FieldValue.ValueCase.DURATION_VALUE ->
        putLong(value.durationValue.seconds).putInt(value.durationValue.nanos)
      FieldValue.ValueCase.TIMESTAMP_VALUE ->
        putLong(value.timestampValue.seconds).putInt(value.timestampValue.nanos)
      FieldValue.ValueCase.VALUE_NOT_SET -> throw IllegalArgumentException("value not set")
    }
  }
  return this // For chaining.
}

private fun ByteBuffer.putBoolean(value: Boolean): ByteBuffer {
  val byte: Byte = if (value) 1 else 0
  return put(byte)
}

private fun ByteBuffer.putStringUtf8(value: String): ByteBuffer {
  return put(value.toByteStringUtf8().asReadOnlyByteBuffer())
}

/**
 * Converts a [CartesianSyntheticEventGroupSpecRecipe] to [SyntheticEventGroupSpec] using a
 * [SyntheticPopulationSpec].
 *
 * For each dateSpec in [the CartesianSyntheticEventGroupSpecRecipe];
 * 1. Group the nonPopulationDimensionSpecs based on their field names. These groups specify the
 *    distribution of the data for the respective dimensions.
 * 2. Cross these groups with each other and the frequencyDimensionSpecs. This produces a desired
 *    distribution for all the non population dimensions and the frequencies.
 * 3. Cross this result with the subPopulations defined in the syntheticPopulationSpec. This will
 *    not add any fields since the poopualtion values are defiend in the syntheticPopulationSpec,
 *    this operation is used to query the vid ranges to be used for the resulting cartesian product.
 * 4. Create frequency specs from these groups such that the frequency field is set from
 *    frequencyDimensionSpec, . the vidRange is the vid Range of the subpopulation and the sampling
 *    rate is the product of all ratios in dimensions and total reach divided by total number of
 *    vids.
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
  check(samplingNonce != 0L) {
    "CartesianSyntheticEventGroupSpecRecipe must specify a samplingNonce"
  }

  val subPopulations = syntheticPopulationSpec.subPopulationsList
  val numAvaliableVids =
    subPopulations.map { it.vidSubRange.endExclusive - it.vidSubRange.start }.sum()

  val mappedDateSpecs =
    this.dateSpecsList.map { dateSpec ->
      val totalReach = dateSpec.totalReach
      check(totalReach <= numAvaliableVids) {
        "CartesianSyntheticEventGroupSpecRecipe implies vids that do not exist"
      }

      // Check if all non Population dimension ratios sum up to 1.
      dateSpec.nonPopulationDimensionSpecsMap.forEach { (fieldName, nonPopulationDimensionSpec) ->
        check(nonPopulationDimensionSpec.fieldValueRatiosList.map { it.ratio }.sum() == 1.0f) {
          "Non population dimension : ${fieldName} does not sum up to 1."
        }
      }

      val groupedNonPopulationDimensionSpecs =
        dateSpec.nonPopulationDimensionSpecsMap
          .flatMap { (fieldName, nonPopulationDimensionSpec) ->
            nonPopulationDimensionSpec.fieldValueRatiosList.map {
              NonPopulationDimension(fieldName, it.fieldValue, it.ratio)
            }
          }
          .groupBy { it.fieldName }

      // Check if FrequencyDimensionSpec ratios sum up to 1.
      check(dateSpec.frequencyRatiosMap.map { it.value }.sum() == 1.0f) {
        "Frequency dimension does not sum up to 1."
      }

      // Take the cartesian product of all non population dimensions
      val nonPopulationCartesianProduct: List<List<NonPopulationDimension>> =
        groupedNonPopulationDimensionSpecs
          .map { (_, group) -> group }
          .fold(listOf(emptyList<NonPopulationDimension>())) { acc, inner ->
            acc.flatMap { outer -> inner.map { element -> outer + listOf(element) } }
          }

      val mappedFrequencySpecs = mutableListOf<SyntheticEventGroupSpec.FrequencySpec>()
      // Take the cartesian product with frequencyDimensionSpecs.
      for ((freq, freqRatio) in dateSpec.frequencyRatiosMap) {
        for (nonPopulationSpecs in nonPopulationCartesianProduct) {

          // Take the cartesian product with syntheticPopulationSpec.
          subPopulations.forEach { subPopulation ->
            // Number of vids in this region is the product of all the ratios in the dimensions.
            val nonPopulationBucketWidth =
              (totalReach *
                freqRatio *
                nonPopulationSpecs.map { it.ratio }.reduce { acc, element -> acc * element })

            // The proportion of vids that should be sampled from this subPopulation.
            // The calculation is as follows:
            // (nonPopulationBucketWidth * (subPopWidth / numAvaliableVids)) / subPopWidth
            val samplingRate = (nonPopulationBucketWidth / numAvaliableVids.toDouble())
            mappedFrequencySpecs +=
              createFrequencySpec(samplingRate, freq, nonPopulationSpecs, subPopulation.vidSubRange)
          }
        }
      }
      SyntheticEventGroupSpecKt.dateSpec {
        dateRange = dateSpec.dateRange
        frequencySpecs += mappedFrequencySpecs
      }
    }
  val givenDescription = this.description
  val givenSamplingNonce = this.samplingNonce
  return syntheticEventGroupSpec {
    description = givenDescription
    samplingNonce = givenSamplingNonce
    dateSpecs += mappedDateSpecs
  }
}

private fun createFrequencySpec(
  rate: Double,
  freq: Long,
  nonPopulationSpecs: List<NonPopulationDimension>,
  range: VidRange,
) =
  SyntheticEventGroupSpecKt.frequencySpec {
    frequency = freq
    vidRangeSpecs +=
      SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
        vidRange = range
        samplingRate = rate
        nonPopulationFieldValues.putAll(
          nonPopulationSpecs.associate { it.fieldName to it.fieldValue }
        )
      }
  }

data class NonPopulationDimension(
  val fieldName: String,
  val fieldValue: FieldValue,
  val ratio: Float,
)
