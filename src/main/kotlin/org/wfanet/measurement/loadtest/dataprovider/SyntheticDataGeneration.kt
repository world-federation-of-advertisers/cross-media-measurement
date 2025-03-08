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
import java.time.Instant
import java.time.ZoneOffset
import kotlin.math.max
import kotlin.math.min
import kotlin.random.Random
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.CartesianSyntheticEventGroupSpecRecipe
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.DateRange
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.FieldValue
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SimulatorSyntheticDataSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticCampaignSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec.FrequencySpec.VidRangeSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpecKt
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec.SubPopulation
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.VidRange
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.syntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.vidRange
import org.wfanet.measurement.common.LocalDateProgression
import org.wfanet.measurement.common.OpenEndTimeRange
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
   * @param timeRange range in which to generate events
   */
  fun <T : Message> generateEvents(
    messageInstance: T,
    populationSpec: SyntheticPopulationSpec,
    syntheticEventGroupSpec: SyntheticEventGroupSpec,
    timeRange: OpenEndTimeRange = OpenEndTimeRange(Instant.MIN, Instant.MAX),
  ): Sequence<LabeledEvent<T>> {
    val subPopulations = populationSpec.subPopulationsList

    return sequence {
      for (dateSpec: SyntheticEventGroupSpec.DateSpec in syntheticEventGroupSpec.dateSpecsList) {
        val dateProgression: LocalDateProgression = dateSpec.dateRange.toProgression()

        // Optimization: Skip the entire DateSpec if it does not overlap the specified time range.
        val dateSpecTimeRange = OpenEndTimeRange.fromClosedDateRange(dateProgression)
        if (!dateSpecTimeRange.overlaps(timeRange)) {
          continue
        }

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
              val timestamp = date.atStartOfDay().toInstant(ZoneOffset.UTC)
              if (timestamp !in timeRange) {
                continue
              }
              for (i in 1..frequencySpec.frequency) {
                for (vid in vidRangeSpec.sampledVids(syntheticEventGroupSpec.samplingNonce)) {
                  yield(LabeledEvent(timestamp, vid, message))
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

private fun DateRange.toProgression(): LocalDateProgression {
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
  val numAvaliableVids = subPopulations.map { it.vidSubRange.size }.sum()

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

      // Check if FrequencyDimensionSpec ratios sum up to 1.
      check(dateSpec.frequencyRatiosMap.map { it.value }.sum() == 1.0f) {
        "Frequency dimension does not sum up to 1."
      }

      val groupedNonPopulationDimensionSpecs =
        dateSpec.nonPopulationDimensionSpecsMap
          .flatMap { (fieldName, nonPopulationDimensionSpec) ->
            nonPopulationDimensionSpec.fieldValueRatiosList.map {
              NonPopulationDimension(fieldName, it.fieldValue, it.ratio)
            }
          }
          .groupBy { it.fieldName }

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

fun SyntheticCampaignSpec.toSyntheticEventGroupSpecs(
  populationSpec: SyntheticPopulationSpec
): List<SyntheticEventGroupSpec> {
  val random = Random(randomSeed)

  check(eventGroupContributionsList.sumOf { it.uniqueReach } >= totalReach) {
    "total_reach cannot be less than sum of unique_reach across EventGroups"
  }
  check(eventGroupContributionsList.sumOf { it.reach } <= totalReach) {
    "total_reach cannot be more than sum of reach across EventGroups"
  }

  val allocatedVidsBySubPopulation = SubPopulationAllocations(populationSpec)

  val eventGroupAllocations: List<EventGroupAllocation> =
    eventGroupContributionsList.map { contributionSpec ->
      check(contributionSpec.reach >= contributionSpec.uniqueReach) {
        "reach cannot be less than unique_reach"
      }
      val coveredReachRatio = contributionSpec.relativeFrequencyDistributionMap.values.sum()
      check(coveredReachRatio == 1.0f) {
        "relative_frequency_distribution only covers ${coveredReachRatio * 100}%"
      }
      val coveredSubPopulationRatio =
        contributionSpec.subPopulationContributionSpecsList.map { it.value.reachRatio }.sum()
      check(coveredSubPopulationRatio == 1.0f) {
        "sub_population_contribution_specs only covers ${coveredSubPopulationRatio * 100}%"
      }

      val subPopulationAllocations:
        Map<SubPopulation, EventGroupAllocation.SubPopulationAllocation> =
        populationSpec.subPopulationsList.associateWith { subPopulation ->
          val allocatedRange = allocatedVidsBySubPopulation[subPopulation]
          val subPopulationSpec = contributionSpec.findSubPopulationSpec(subPopulation)
          if (subPopulationSpec == null) {
            EventGroupAllocation.SubPopulationAllocation(0L, allocatedRange)
          } else {
            val minVidCount: Long =
              (subPopulationSpec.value.reachRatio * contributionSpec.reach).toLong()
            check(minVidCount <= subPopulation.vidSubRange.size) {
              "SubPopulation only has ${subPopulation.vidSubRange.size} VIDs; need at least $minVidCount"
            }

            // Allocate unique VIDs.
            val uniqueVidCount =
              (subPopulationSpec.value.reachRatio * contributionSpec.uniqueReach).toLong()
            val uniqueVidRange =
              allocatedVidsBySubPopulation.allocate(subPopulation, uniqueVidCount)
            EventGroupAllocation.SubPopulationAllocation(
              minVidCount - uniqueVidCount,
              uniqueVidRange,
            )
          }
        }
      EventGroupAllocation(contributionSpec, subPopulationAllocations)
    }

  // Allocate overlapping VIDs.
  while (!eventGroupAllocations.all { it.fullyAllocated }) {
    for (subPopulation in populationSpec.subPopulationsList) {
      val sortedAllocations: List<EventGroupAllocation.SubPopulationAllocation> =
        buildList {
            for (eventGroupAllocation in eventGroupAllocations) {
              add(eventGroupAllocation.subPopulationAllocations.getValue(subPopulation))
            }
          }
          .sortedBy { it.unallocatedOverlappingCount }

      // Allocate in first EventGroup sub-population.
      val subPopulationAllocation = sortedAllocations.first()
      val overlappingRange =
        allocatedVidsBySubPopulation.allocate(
          subPopulation,
          subPopulationAllocation.unallocatedOverlappingCount,
        )
      subPopulationAllocation.overlappingVidRanges.add(overlappingRange)

      // Overlap with a random number of additional EventGroups.
      val additionalEventGroupCount = random.nextInt(1, sortedAllocations.size - 1)
      for (i in 1..additionalEventGroupCount) {
        sortedAllocations[i].overlappingVidRanges.add(overlappingRange)
      }
    }
  }

  // TODO: Distribute impressions across non-population fields.
  // TODO: Distribute impressions across dates.
  // TODO: Generate SyntheticEventGroupSpecs.

  TODO()
}

private data class EventGroupAllocation(
  val contributionSpec: SyntheticCampaignSpec.EventGroupContributionSpec,
  val subPopulationAllocations: Map<SubPopulation, SubPopulationAllocation>,
) {
  val fullyAllocated: Boolean
    get() = subPopulationAllocations.values.all { it.fullyAllocated }

  /** Sub-population VID allocation for a single EventGroup. */
  data class SubPopulationAllocation(
    val overlappingCount: Long,
    val uniqueVidRange: VidRange,
    val overlappingVidRanges: MutableList<VidRange> = mutableListOf(),
  ) {
    val allocatedOverlappingCount: Long
      get() = overlappingVidRanges.sumOf { it.size }

    val unallocatedOverlappingCount: Long
      get() = overlappingCount - allocatedOverlappingCount

    val fullyAllocated: Boolean
      get() = unallocatedOverlappingCount == 0L
  }
}

/** Sub-population VID allocations across all EventGroups. */
private class SubPopulationAllocations(populationSpec: SyntheticPopulationSpec) {
  private val allocatedVids = mutableMapOf<SubPopulation, VidRange>()

  init {
    for (subPopulation in populationSpec.subPopulationsList) {
      allocatedVids[subPopulation] = vidRange {
        start = subPopulation.vidSubRange.start
        endExclusive = subPopulation.vidSubRange.start
      }
    }
  }

  fun allocate(subPopulation: SubPopulation, count: Long): VidRange {
    val existingRange = allocatedVids.getValue(subPopulation)
    allocatedVids[subPopulation] = existingRange.extended(count)
    return existingRange.nextRange(count)
  }

  operator fun get(subPopulation: SubPopulation): VidRange = allocatedVids.getValue(subPopulation)
}

private val VidRange.size
  get() = endExclusive - start

/** Returns a range of size [count] starting at the end of this one. */
private fun VidRange.nextRange(count: Long): VidRange {
  val source = this
  return vidRange {
    start = source.endExclusive
    endExclusive = source.endExclusive + count
  }
}

/** Returns a range that has its end extended by [count]. */
private fun VidRange.extended(count: Long): VidRange {
  val source = this
  return vidRange {
    start = source.start
    endExclusive = source.endExclusive + count
  }
}

private fun SyntheticCampaignSpec.EventGroupContributionSpec.findSubPopulationSpec(
  subPopulation: SubPopulation
) =
  subPopulationContributionSpecsList.find {
    it.key.fieldValuesMap == subPopulation.populationFieldsValuesMap
  }
