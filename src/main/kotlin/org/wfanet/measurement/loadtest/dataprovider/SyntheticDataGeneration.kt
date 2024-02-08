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
