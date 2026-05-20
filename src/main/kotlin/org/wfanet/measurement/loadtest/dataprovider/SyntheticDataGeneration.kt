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

package org.wfanet.measurement.loadtest.dataprovider

import com.google.common.hash.Hashing
import com.google.protobuf.Descriptors.FieldDescriptor
import com.google.protobuf.Message
import com.google.protobuf.kotlin.toByteStringUtf8
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import java.util.logging.Logger
import kotlin.math.abs
import kotlin.math.max
import kotlin.math.min
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.FieldValue
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec.FrequencySpec.VidRangeSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.VidRange
import org.wfanet.measurement.common.LocalDateProgression
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.overlaps
import org.wfanet.measurement.common.rangeTo
import org.wfanet.measurement.common.toByteString
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.common.toOpenEndInstantRange

object SyntheticDataGeneration {
  private val VID_SAMPLING_FINGERPRINT_FUNCTION = Hashing.farmHashFingerprint64()
  private const val FINGERPRINT_BUFFER_SIZE_BYTES = 512
  private const val SECONDS_PER_DAY = 86400

  /**
   * Generates events deterministically from a v2alpha [PopulationSpec]. Given a total frequency
   * across a date period, it will generate events across that time period based on a hash function.
   * For example, for a user with frequency of 5, over a 10-day period, that user will have exactly
   * 5 events with their VID in the output over the 10-day period.
   *
   * The event message type is taken from [messageInstance]'s descriptor. Each
   * [PopulationSpec.SubPopulation.attributes] entry must be a [com.google.protobuf.Any] holding an
   * event template message whose type matches one of the template fields of the event message (e.g.
   * for `TestEvent` an attribute may carry a `Person` message). Population field values are taken
   * from these attribute messages; non-population field values are taken from
   * [VidRangeSpec.nonPopulationFieldValuesMap], whose keys are template field paths of the form
   * `<template_name>.<field_name>` (or `<template_name>.<sub_message>.<field_name>` for nested
   * scalar fields).
   *
   * @param messageInstance an instance of the event message type [T]
   * @param populationSpec specification of the population (v2alpha)
   * @param syntheticEventGroupSpec specification of the synthetic event group
   * @param timeRange range in which to generate events
   * @param zoneId timezone for date shards
   */
  fun <T : Message> generateEvents(
    messageInstance: T,
    populationSpec: PopulationSpec,
    syntheticEventGroupSpec: SyntheticEventGroupSpec,
    timeRange: OpenEndRange<Instant> = Instant.MIN..<Instant.MAX,
    zoneId: ZoneId = ZoneOffset.UTC,
  ): Sequence<LabeledEventDateShard<T>> {
    val context = PopulationSpecGenerationContext(messageInstance, populationSpec)
    return sequence {
      for (dateSpec: SyntheticEventGroupSpec.DateSpec in syntheticEventGroupSpec.dateSpecsList) {
        val dateProgression: LocalDateProgression = dateSpec.dateRange.toProgression()

        // Optimization: Skip the entire DateSpec if it does not overlap the specified time range.
        val dateSpecTimeRange: OpenEndRange<Instant> = dateProgression.toOpenEndInstantRange()
        if (!dateSpecTimeRange.overlaps(timeRange)) {
          continue
        }
        val numDays =
          ChronoUnit.DAYS.between(dateProgression.start, dateProgression.endInclusive) + 1
        logger.info("Writing $numDays days of data")
        for (date in dateProgression) {
          val events: Sequence<LabeledEvent<T>> =
            generateDayEvents(
              context,
              dateProgression,
              date,
              zoneId,
              syntheticEventGroupSpec,
              dateSpec,
              numDays.toInt(),
              timeRange,
            )
          yield(LabeledEventDateShard(date, events))
        }
      }
    }
  }

  /**
   * Holds per-invocation derived data shared across all date shards: the prebuilt subpopulation
   * "prototype" messages with all population template attributes already merged in, and the lookup
   * tables for finding the right subpopulation for a given [VidRangeSpec].
   */
  private class PopulationSpecGenerationContext<T : Message>(
    val messageInstance: T,
    populationSpec: PopulationSpec,
  ) {
    private val templateFieldsByTypeUrl: Map<String, FieldDescriptor> =
      buildTemplateFieldsByTypeUrl(messageInstance)

    /**
     * Pre-built prototypes for each [PopulationSpec.SubPopulation], with all population attribute
     * template messages merged into the event message. Indexed by subpopulation index.
     */
    val subPopulationPrototypes: List<T> =
      populationSpec.subpopulationsList.map { buildPrototype(it) }

    /**
     * Per-subpopulation list of [PopulationSpec.VidRange]s, used to find which subpopulation a
     * given [VidRangeSpec.vidRange] belongs to.
     */
    private val subPopulationVidRanges: List<List<PopulationSpec.VidRange>> =
      populationSpec.subpopulationsList.map { it.vidRangesList }

    /**
     * Returns the index of the subpopulation that fully contains [vidRange], or null if none does.
     */
    fun findSubPopulationIndex(vidRange: VidRange): Int? {
      for ((index, vidRanges) in subPopulationVidRanges.withIndex()) {
        for (subRange in vidRanges) {
          // VidRangeSpec.vidRange.endExclusive is exclusive;
          // PopulationSpec.VidRange.endVidInclusive
          // is inclusive. The spec range fits inside the subpopulation range when:
          //   spec.start >= sub.startVid && spec.endExclusive - 1 <= sub.endVidInclusive
          if (
            vidRange.start >= subRange.startVid &&
              vidRange.endExclusive - 1 <= subRange.endVidInclusive
          ) {
            return index
          }
        }
      }
      return null
    }

    private fun buildPrototype(subPopulation: PopulationSpec.SubPopulation): T {
      val builder = messageInstance.newBuilderForType()
      for (attribute in subPopulation.attributesList) {
        val templateField =
          templateFieldsByTypeUrl[attribute.typeUrl]
            ?: throw IllegalArgumentException(
              "Attribute type_url ${attribute.typeUrl} does not correspond to any template " +
                "field of ${messageInstance.descriptorForType.fullName}"
            )
        builder.getFieldBuilder(templateField).mergeFrom(attribute.value)
      }
      @Suppress("UNCHECKED_CAST") // Safe per protobuf API.
      return builder.build() as T
    }

    private fun buildTemplateFieldsByTypeUrl(messageInstance: T): Map<String, FieldDescriptor> =
      buildMap {
        for (field in messageInstance.descriptorForType.fields) {
          if (field.type != FieldDescriptor.Type.MESSAGE) {
            continue
          }
          val typeUrl = ProtoReflection.getTypeUrl(field.messageType)
          check(typeUrl !in this) {
            "Duplicate template type URL $typeUrl: fields '${this[typeUrl]!!.name}' and " +
              "'${field.name}'"
          }
          put(typeUrl, field)
        }
      }
  }

  private fun <T : Message> generateDayEvents(
    context: PopulationSpecGenerationContext<T>,
    dateProgression: LocalDateProgression,
    date: LocalDate,
    zoneId: ZoneId,
    syntheticEventGroupSpec: SyntheticEventGroupSpec,
    dateSpec: SyntheticEventGroupSpec.DateSpec,
    numDays: Int,
    timeRange: OpenEndRange<Instant>,
  ): Sequence<LabeledEvent<T>> = sequence {
    val dayNumber = ChronoUnit.DAYS.between(dateProgression.start, date)
    logger.info("Generating data for day: $dayNumber date: $date")
    for (frequencySpec: SyntheticEventGroupSpec.FrequencySpec in dateSpec.frequencySpecsList) {

      check(!frequencySpec.hasOverlaps()) { "The VID ranges should be non-overlapping." }

      for (vidRangeSpec: VidRangeSpec in frequencySpec.vidRangeSpecsList) {
        val subPopulationIndex: Int =
          context.findSubPopulationIndex(vidRangeSpec.vidRange)
            ?: error("Sub-population not found for ${vidRangeSpec.vidRange}")
        check(vidRangeSpec.samplingRate in 0.0..1.0) { "Invalid sampling_rate" }
        if (vidRangeSpec.sampled) {
          check(syntheticEventGroupSpec.samplingNonce != 0L) {
            "sampling_nonce is required for VID sampling"
          }
        }

        val builder: Message.Builder =
          context.subPopulationPrototypes[subPopulationIndex].toBuilder()

        for ((path, fieldValue) in vidRangeSpec.nonPopulationFieldValuesMap.entries) {
          val fieldPath = path.split('.')
          try {
            builder.setField(fieldPath, fieldValue)
          } catch (e: IllegalArgumentException) {
            throw IllegalStateException(e)
          }
        }

        @Suppress("UNCHECKED_CAST") // Safe per protobuf API.
        val message = builder.build() as T
        for (vid in vidRangeSpec.sampledVids(syntheticEventGroupSpec.samplingNonce)) {
          for (i in 1..frequencySpec.frequency) {
            val dayToLog =
              (VID_SAMPLING_FINGERPRINT_FUNCTION.hashLong(vid * i).asLong() % numDays + numDays) %
                numDays
            if (dayToLog == dayNumber) {
              val hashInput =
                vid
                  .toByteString(ByteOrder.BIG_ENDIAN)
                  .concat(dayToLog.toByteString(ByteOrder.BIG_ENDIAN))
              val hashValue =
                abs(Hashing.farmHashFingerprint64().hashBytes(hashInput.toByteArray()).asLong())
              val impressionTime =
                date.atStartOfDay(zoneId).plusSeconds(hashValue % SECONDS_PER_DAY)
              if (impressionTime.toInstant() in timeRange) {
                yield(LabeledEvent(impressionTime.toInstant(), vid, message))
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

  private val logger: Logger = Logger.getLogger(this::class.java.name)
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
