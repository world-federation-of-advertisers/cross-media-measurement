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
import kotlin.jvm.java
import kotlin.math.abs
import kotlin.math.max
import kotlin.math.min
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.flatMapMerge
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.FieldValue
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec.FrequencySpec.VidRangeSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec.SubPopulation
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.VidRange
import org.wfanet.measurement.common.LocalDateProgression
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.rangeTo
import org.wfanet.measurement.common.toByteString
import org.wfanet.measurement.common.toLocalDate

/**
 * Extension function for batching Flow elements into lists of specified size.
 *
 * ## Batching Strategy
 * - Collects elements into batches of the specified size
 * - Emits the final batch even if it's smaller than the batch size
 * - Useful for processing large streams in manageable chunks
 *
 * @param size the maximum number of elements per batch (must be > 0)
 * @return flow of lists containing batched elements
 * @throws IllegalArgumentException if size <= 0
 */
fun <T> Flow<T>.batch(size: Int): Flow<List<T>> = flow {
  require(size > 0) { "Batch size must be > 0" }
  val buffer = ArrayList<T>(size)
  collect { value ->
    buffer += value
    if (buffer.size == size) {
      emit(buffer.toList())
      buffer.clear()
    }
  }
  if (buffer.isNotEmpty()) emit(buffer.toList())
}

/**
 * Utility object for generating synthetic event data deterministically.
 *
 * ## Algorithm Overview
 *
 * This generator creates synthetic events using a deterministic hash-based algorithm that ensures:
 * - Reproducibility: Same inputs always produce identical outputs
 * - Distribution: Events are evenly distributed across time periods
 * - Frequency accuracy: Each VID generates exactly the specified number of events
 *
 * ## Core Algorithm
 *
 * The event generation uses FarmHash fingerprinting to deterministically assign events to days:
 * 1. For each VID and frequency count, compute: `hash(vid * frequency_index) % num_days`
 * 2. This determines which day in the date range gets that event
 * 3. Within each day, use another hash to determine the exact timestamp
 *
 * ## Parallelization Strategy
 *
 * The implementation uses Kotlin coroutines for parallel processing:
 * - Dates are chunked and processed in parallel using available CPU cores
 * - Each core processes its assigned dates independently
 * - Results are collected and sorted chronologically
 *
 * ## Sampling Mechanism
 *
 * VID sampling uses a consistent hash-based approach:
 * - Computes fingerprint using VID, sampling nonce, and field values
 * - Maps fingerprint to [-1, 1] range and compares with sampling rate
 * - Ensures consistent sampling across different runs with same nonce
 *
 * @property VID_SAMPLING_FINGERPRINT_FUNCTION FarmHash function for consistent hashing
 * @property FINGERPRINT_BUFFER_SIZE_BYTES Buffer size for fingerprint computation
 * @property SECONDS_PER_DAY Number of seconds per day for timestamp distribution
 */
object SyntheticDataGeneration {
  private val VID_SAMPLING_FINGERPRINT_FUNCTION = Hashing.farmHashFingerprint64()
  private const val FINGERPRINT_BUFFER_SIZE_BYTES = 512
  private const val SECONDS_PER_DAY = 86400

  /**
   * Generates synthetic events deterministically using a hash-based distribution algorithm.
   *
   * This function serves as the main entry point for synthetic event generation. It delegates to
   * [generateEventsParallel] for efficient parallel processing.
   *
   * ## Algorithm Details
   *
   * Given a total frequency across a date period, events are distributed deterministically:
   * - Each VID's events are spread across the date range using hash-based assignment
   * - For a VID with frequency 5 over 10 days, exactly 5 events will be generated
   * - Event timestamps within each day are also deterministically assigned
   *
   * @param T the type of event message, must extend [Message]
   * @param messageInstance prototype instance of the event message type
   * @param populationSpec defines the synthetic population including VID ranges and field values
   * @param syntheticEventGroupSpec defines event generation parameters (dates, frequencies,
   *   sampling)
   * @param timeRange optional time bounds for generated events (defaults to all time)
   * @param zoneId timezone for date shard calculations (defaults to UTC)
   * @return sequence of [LabeledEventDateShard] containing events grouped by date
   */
  fun <T : Message> generateEvents(
    messageInstance: T,
    populationSpec: SyntheticPopulationSpec,
    syntheticEventGroupSpec: SyntheticEventGroupSpec,
    timeRange: OpenEndTimeRange = OpenEndTimeRange(Instant.MIN, Instant.MAX),
    zoneId: ZoneId = ZoneOffset.UTC,
  ): Sequence<LabeledEventDateShard<T>> {
    return generateEventsParallel(
      messageInstance,
      populationSpec,
      syntheticEventGroupSpec,
      timeRange,
      zoneId,
    )
  }

  /**
   * Generates synthetic events using parallel processing for improved performance.
   *
   * ## Parallelization Strategy
   *
   * This function leverages all available CPU cores to process dates in parallel:
   * 1. Divides the date range into chunks based on available processors
   * 2. Each chunk is processed independently on a coroutine
   * 3. Results are collected and sorted chronologically
   *
   * ## Performance Characteristics
   * - Scales linearly with CPU cores for large date ranges
   * - Memory usage is controlled through streaming/flow processing
   * - Maintains deterministic output despite parallel execution
   *
   * ## Implementation Notes
   *
   * The function uses Kotlin coroutines with the Default dispatcher for CPU-bound work. Work
   * stealing ensures efficient load balancing across cores.
   *
   * @param T the type of event message, must extend [Message]
   * @param messageInstance prototype instance of the event message type
   * @param populationSpec defines the synthetic population
   * @param syntheticEventGroupSpec defines event generation parameters
   * @param timeRange optional time bounds for generated events
   * @param zoneId timezone for date calculations
   * @return sequence of date shards containing generated events, sorted by date
   */
  fun <T : Message> generateEventsParallel(
    messageInstance: T,
    populationSpec: SyntheticPopulationSpec,
    syntheticEventGroupSpec: SyntheticEventGroupSpec,
    timeRange: OpenEndTimeRange = OpenEndTimeRange(Instant.MIN, Instant.MAX),
    zoneId: ZoneId = ZoneOffset.UTC,
  ): Sequence<LabeledEventDateShard<T>> {
    val numCores = Runtime.getRuntime().availableProcessors()

    return runBlocking {
      val allShards = mutableListOf<LabeledEventDateShard<T>>()
      createDateShardFlow(
          messageInstance,
          populationSpec,
          syntheticEventGroupSpec,
          timeRange,
          zoneId,
          numCores,
        )
        .collect { shard ->
          val events = shard.labeledEvents.toList().asSequence()
          allShards.add(LabeledEventDateShard(shard.localDate, events))
        }
      allShards.sortedBy { it.localDate }.asSequence()
    }
  }

  /**
   * Generates synthetic events as a Flow for streaming processing.
   *
   * ## Flow-Based Processing
   *
   * This function provides a streaming alternative to the sequence-based approach:
   * - Events are generated and emitted as they become available
   * - Memory efficient for large datasets through streaming
   * - Enables backpressure handling and reactive processing
   * - Maintains the same deterministic generation algorithm
   *
   * ## Use Cases
   * - Large dataset processing where memory usage matters
   * - Integration with reactive systems requiring Flow types
   * - Scenarios requiring fine-grained control over processing pace
   *
   * @param T the type of event message, must extend [Message]
   * @param messageInstance prototype instance of the event message type
   * @param populationSpec defines the synthetic population
   * @param syntheticEventGroupSpec defines event generation parameters
   * @param timeRange optional time bounds for generated events
   * @param zoneId timezone for date calculations
   * @return flow of date shards containing generated events
   */
  fun <T : Message> generateEventsFlow(
    messageInstance: T,
    populationSpec: SyntheticPopulationSpec,
    syntheticEventGroupSpec: SyntheticEventGroupSpec,
    timeRange: OpenEndTimeRange = OpenEndTimeRange(Instant.MIN, Instant.MAX),
    zoneId: ZoneId = ZoneOffset.UTC,
  ): Flow<LabeledEventDateShardFlow<T>> {
    val numCores = Runtime.getRuntime().availableProcessors()

    return createDateShardFlow(
      messageInstance,
      populationSpec,
      syntheticEventGroupSpec,
      timeRange,
      zoneId,
      numCores,
    )
  }

  /**
   * Creates a flow of date shards with parallel processing for each date specification.
   *
   * ## Processing Flow
   * 1. Iterates through each DateSpec in the event group specification
   * 2. Skips DateSpecs that don't overlap with the requested time range (optimization)
   * 3. Chunks dates for parallel processing based on available cores
   * 4. Uses `flatMapMerge` for concurrent processing with controlled parallelism
   *
   * ## Optimization Details
   * - Early filtering: Skip entire DateSpecs that don't overlap the time range
   * - Dynamic chunking: Adjusts chunk size based on date range and core count
   * - Work stealing: Dispatcher.Default provides efficient work distribution
   *
   * @param T the type of event message
   * @param messageInstance prototype for creating events
   * @param populationSpec population configuration
   * @param syntheticEventGroupSpec event generation configuration
   * @param timeRange bounds for event generation
   * @param zoneId timezone for date calculations
   * @param numCores number of CPU cores for parallel processing
   * @return flow of date shards containing generated events
   */
  @OptIn(ExperimentalCoroutinesApi::class)
  private fun <T : Message> createDateShardFlow(
    messageInstance: T,
    populationSpec: SyntheticPopulationSpec,
    syntheticEventGroupSpec: SyntheticEventGroupSpec,
    timeRange: OpenEndTimeRange,
    zoneId: ZoneId,
    numCores: Int,
  ): Flow<LabeledEventDateShardFlow<T>> =
    syntheticEventGroupSpec.dateSpecsList.asFlow().flatMapConcat { dateSpec ->
      val dateProgression: LocalDateProgression = dateSpec.dateRange.toProgression()

      // Optimization: Skip the entire DateSpec if it does not overlap the specified time range.
      val dateSpecTimeRange = OpenEndTimeRange.fromClosedDateRange(dateProgression)
      if (!dateSpecTimeRange.overlaps(timeRange)) {
        emptyFlow()
      } else {
        val numDays =
          ChronoUnit.DAYS.between(dateProgression.start, dateProgression.endInclusive) + 1
        logger.info("Writing $numDays days of data using $numCores cores")

        dateProgression.asFlow().flatMapMerge(concurrency = numCores) { date ->
          flow {
            val eventsFlow: Flow<LabeledEvent<T>> =
              generateDayEvents(
                dateProgression,
                date,
                zoneId,
                messageInstance,
                syntheticEventGroupSpec,
                dateSpec,
                populationSpec,
                numDays.toInt(),
                timeRange,
              )
            emit(LabeledEventDateShardFlow(date, eventsFlow))
          }
        }
      }
    }

  /**
   * Generates all events for a specific day using the deterministic hash algorithm.
   *
   * ## Day Assignment Algorithm
   *
   * For each VID and frequency combination:
   * 1. Compute day assignment: `hash(vid * frequency_index) % num_days`
   * 2. If assigned day matches current day, generate event
   * 3. Timestamp within day: `hash(vid || day) % seconds_per_day`
   *
   * ## Event Construction Process
   * 1. Find matching sub-population for VID range
   * 2. Apply population fields from sub-population
   * 3. Apply non-population fields from VID range spec
   * 4. Check sampling criteria if applicable
   * 5. Generate event with deterministic timestamp
   *
   * ## Sampling Logic
   * - If sampling_rate = 1.0: Include all VIDs
   * - If 0 < sampling_rate < 1.0: Use hash-based sampling
   * - Sampling requires a non-zero sampling_nonce for consistency
   *
   * @param T the type of event message
   * @param dateProgression the full date range being processed
   * @param date the specific date to generate events for
   * @param zoneId timezone for timestamp calculations
   * @param messageInstance prototype for event creation
   * @param syntheticEventGroupSpec event generation configuration
   * @param dateSpec date-specific configuration
   * @param populationSpec population configuration
   * @param numDays total number of days in the range
   * @param timeRange time bounds for filtering events
   * @return flow of labeled events for the specified day
   */
  private fun <T : Message> generateDayEvents(
    dateProgression: LocalDateProgression,
    date: LocalDate,
    zoneId: ZoneId,
    messageInstance: T,
    syntheticEventGroupSpec: SyntheticEventGroupSpec,
    dateSpec: SyntheticEventGroupSpec.DateSpec,
    populationSpec: SyntheticPopulationSpec,
    numDays: Int,
    timeRange: OpenEndTimeRange,
  ): Flow<LabeledEvent<T>> = flow {
    val subPopulations = populationSpec.subPopulationsList
    val dayNumber = ChronoUnit.DAYS.between(dateProgression.start, date)
    logger.info("Generating data for day: $dayNumber date: $date")
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
                emit(LabeledEvent(impressionTime.toInstant(), vid, message))
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

  /**
   * Returns true if the given [vid] should be included in the sample using consistent hashing.
   *
   * ## Sampling Algorithm
   * 1. Combine [vid], [samplingNonce], and field values into a buffer
   * 2. Compute FarmHash fingerprint of the buffer
   * 3. Map fingerprint to [-1, 1] range: `fingerprint / Long.MAX_VALUE`
   * 4. Include if value falls within [-sampling_rate, sampling_rate]
   *
   * This ensures:
   * - Consistent sampling across different runs with same nonce
   * - Uniform distribution of sampled VIDs
   * - Field values affect sampling for additional randomization
   */
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
   * Sets a [fieldValue] in a Protocol Buffer message builder using a [fieldPath]
   *
   * ## Field Path Resolution
   *
   * Supports nested field access using dot notation:
   * - Single field: `"field_name"` → direct field access
   * - Nested field: `"parent.child"` → recursive traversal
   *
   * @throws IllegalArgumentException if field type doesn't match or value not set
   * @throws NullPointerException if field path is invalid
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

/** Converts a DateRange protocol buffer to a LocalDateProgression. */
private fun SyntheticEventGroupSpec.DateSpec.DateRange.toProgression(): LocalDateProgression {
  return start.toLocalDate()..endExclusive.toLocalDate().minusDays(1)
}

/**
 * Returns true if any VID ranges in this frequency specification overlap.
 *
 * ## Overlap Detection Algorithm
 * Sort the ranges by their start. If there are any consecutive ranges where
 * the previous has a larger end than the latter's start, then there is an overlap.
 */
private fun SyntheticEventGroupSpec.FrequencySpec.hasOverlaps() =
  vidRangeSpecsList
    .map { it.vidRange }
    .sortedBy { it.start }
    .zipWithNext()
    .any { (first, second) -> first.overlaps(second) }

/** Returns true if this VID range overlaps with [other] range. */
private fun VidRange.overlaps(other: VidRange) =
  max(start, other.start) < min(endExclusive, other.endExclusive)

/**
 * Serializes a [fieldValueMap] into this ByteBuffer for fingerprinting.
 *
 * ## Serialization Format
 *
 * Fields are serialized in sorted order by key to ensure consistency:
 * 1. Field name as UTF-8 string
 * 2. Field value based on type (preserving type-specific encoding)
 *
 * This deterministic serialization ensures consistent fingerprints across different runs with the
 * same data.
 *
 * @throws IllegalArgumentException if a field value is not set
 */
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

/** Writes a boolean [value] to this ByteBuffer as a byte. */
private fun ByteBuffer.putBoolean(value: Boolean): ByteBuffer {
  val byte: Byte = if (value) 1 else 0
  return put(byte)
}

/** Writes a UTF-8 encoded string [value] to this ByteBuffer. */
private fun ByteBuffer.putStringUtf8(value: String): ByteBuffer {
  return put(value.toByteStringUtf8().asReadOnlyByteBuffer())
}
