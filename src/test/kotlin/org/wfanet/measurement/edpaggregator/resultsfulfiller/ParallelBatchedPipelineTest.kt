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

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.common.truth.Truth.assertThat
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.ByteString
import com.google.protobuf.DynamicMessage
import com.google.protobuf.TypeRegistry
import com.google.type.Interval
import java.nio.file.Files
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.flow.Flow
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.EncryptedStorage
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.testing.TestEncryptedStorage
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.blobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.labeledImpression
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.VidIndexMap
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.InMemoryVidIndexMap
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.impression
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.subPopulation
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.vidRange
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import kotlin.system.measureTimeMillis

// Extension functions for FrequencyVector to support test expectations
fun FrequencyVector.getReach(): Long {
  return getArray().count { it > 0 }.toLong()
}

fun FrequencyVector.getTotalCount(): Long {
  return getArray().sum().toLong()
}

fun FrequencyVector.getAverageFrequency(): Double {
  val reach = getReach()
  return if (reach > 0) getTotalCount().toDouble() / reach else 0.0
}

@RunWith(JUnit4::class)
class ParallelBatchedPipelineTest {

  @get:Rule val tempFolder = TemporaryFolder()

  private lateinit var typeRegistry: TypeRegistry

  private val realVidIndexMap: VidIndexMap = InMemoryVidIndexMap.build(TEST_POPULATION_SPEC)

  private val testTypeRegistry: TypeRegistry = TypeRegistry.newBuilder()
    .add(TestEvent.getDescriptor())
    .build()

  init {
    AeadConfig.register()
    StreamingAeadConfig.register()
  }

  @Before
  fun setUp() {
    // Set up TypeRegistry
    typeRegistry = TypeRegistry.newBuilder()
      .add(TestEvent.getDescriptor())
      .build()
  }

  private fun createDynamicMessage(testEvent: TestEvent): DynamicMessage {
    return DynamicMessage.newBuilder(TestEvent.getDescriptor())
      .mergeFrom(testEvent)
      .build()
  }

  private class TestEventSource(
    private val eventBatches: List<List<LabeledEvent<out com.google.protobuf.Message>>>
  ) : EventSource {
    override suspend fun generateEventBatches(
      dispatcher: CoroutineContext
    ): Flow<EventBatch> {
      return eventBatches.asFlow().map { eventList ->
        val events = eventList.map { it as LabeledEvent<com.google.protobuf.Message> }
        // Use the actual event timestamps for minTime and maxTime
        val minTime = if (events.isNotEmpty()) {
          events.minOf { it.timestamp }
        } else {
          java.time.Instant.now()
        }
        val maxTime = if (events.isNotEmpty()) {
          events.maxOf { it.timestamp }.plusSeconds(1) // Add 1 second to make it exclusive
        } else {
          java.time.Instant.now().plusSeconds(1)
        }
        EventBatch(
          events = events,
          minTime = minTime,
          maxTime = maxTime
        )
      }
    }
  }

  @Test
  fun `processEventBatches processes empty flow successfully`() = runBlocking {
    val pipeline = ParallelBatchedPipeline(
      batchSize = 10,
      workers = 2,
      dispatcher = Dispatchers.Default
    )

    // Create a wide time interval for testing
    val now = Instant.now()
    val timeInterval = Interval.newBuilder()
      .setStartTime(com.google.protobuf.Timestamp.newBuilder()
        .setSeconds(now.epochSecond - 86400)) // 1 day before
      .setEndTime(com.google.protobuf.Timestamp.newBuilder()
        .setSeconds(now.epochSecond + 86400)) // 1 day after
      .build()

    val sinks = listOf(
      FrequencyVectorSink(
        frequencyVector = StripedByteFrequencyVector(TEST_POPULATION_SPEC, TEST_MEASUREMENT_SPEC),
        vidIndexMap = realVidIndexMap,
        filterProcessor = FilterProcessor(
          filterSpec = FilterSpec(
            celExpression = "", // Empty expression matches all events
            collectionInterval = timeInterval,
            eventGroupReferenceIds = listOf("reference-id-1"),
          ),
          eventDescriptor = TestEvent.getDescriptor()
        )
      )
    )

    pipeline.processEventBatches(
      eventSource = TestEventSource(emptyList()),
      sinks = sinks
    )

    assertThat(sinks).hasSize(1)
  }

  @Test
  fun `processEventBatches handles single event`() = runBlocking {
    val pipeline = ParallelBatchedPipeline(
      batchSize = 10,
      workers = 2,
      dispatcher = Dispatchers.Default
    )

    val testEvent = TestEvent.newBuilder()
      .setPerson(Person.newBuilder().setAgeGroup(Person.AgeGroup.YEARS_18_TO_34))
      .build()

    val eventTimestamp = Instant.now()
    val labeledEvent = LabeledEvent(
      timestamp = eventTimestamp,
      vid = 500L, // Within our TEST_POPULATION_SPEC range (1-1000)
      message = createDynamicMessage(testEvent),
      eventGroupReferenceId = "reference-id-1"
    )

    // Create a time interval that includes the current event
    val timeInterval = Interval.newBuilder()
      .setStartTime(com.google.protobuf.Timestamp.newBuilder()
        .setSeconds(eventTimestamp.epochSecond - 3600)) // 1 hour before event
      .setEndTime(com.google.protobuf.Timestamp.newBuilder()
        .setSeconds(eventTimestamp.epochSecond + 3600)) // 1 hour after event
      .build()

    val sinks = listOf(
      FrequencyVectorSink(
        frequencyVector = StripedByteFrequencyVector(TEST_POPULATION_SPEC, TEST_MEASUREMENT_SPEC),
        vidIndexMap = realVidIndexMap,
        filterProcessor = FilterProcessor(
          filterSpec = FilterSpec(
            celExpression = "", // Empty expression matches all events
            collectionInterval = timeInterval,
            eventGroupReferenceIds = listOf("reference-id-1"),
          ),
          eventDescriptor = TestEvent.getDescriptor()
        )
      )
    )

    pipeline.processEventBatches(
      eventSource = TestEventSource(listOf(listOf(labeledEvent))),
      sinks = sinks
    )

    assertThat(sinks).hasSize(1)
    val frequencyVector = sinks.first().getFrequencyVector()
    assertThat(frequencyVector.getReach()).isEqualTo(1L)
  }

  @Test
  fun `processEventBatches with time interval filters`() = runBlocking {
    val pipeline = ParallelBatchedPipeline(
      batchSize = 5,
      workers = 1,
      dispatcher = Dispatchers.Default
    )

    val testEvent = TestEvent.newBuilder()
      .setPerson(Person.newBuilder().setAgeGroup(Person.AgeGroup.YEARS_18_TO_34))
      .build()

    val labeledEvent = LabeledEvent(
      timestamp = Instant.now(),
      vid = 600L, // Within our TEST_POPULATION_SPEC range (1-1000)
      message = createDynamicMessage(testEvent),
      eventGroupReferenceId = "reference-id-1"
    )

    val timeInterval = Interval.newBuilder()
      .setStartTime(com.google.protobuf.Timestamp.newBuilder()
        .setSeconds(System.currentTimeMillis() / 1000 - 3600))
      .setEndTime(com.google.protobuf.Timestamp.newBuilder()
        .setSeconds(System.currentTimeMillis() / 1000))
      .build()

    val sinks = listOf(
      FrequencyVectorSink(
        frequencyVector = StripedByteFrequencyVector(TEST_POPULATION_SPEC, TEST_MEASUREMENT_SPEC),
        vidIndexMap = realVidIndexMap,
        filterProcessor = FilterProcessor(
          filterSpec = FilterSpec(
            celExpression = "", // Empty expression matches all events
            collectionInterval = timeInterval,
            eventGroupReferenceIds = listOf("reference-id-1"),
          ),
          eventDescriptor = TestEvent.getDescriptor()
        )
      )
    )

    pipeline.processEventBatches(
      eventSource = TestEventSource(listOf(listOf(labeledEvent))),
      sinks = sinks
    )

    assertThat(sinks).hasSize(1)
  }

  @Test
  fun `processEventBatches with different batch sizes`() = runBlocking {
    val smallBatchPipeline = ParallelBatchedPipeline(
      batchSize = 2,
      workers = 1,
      dispatcher = Dispatchers.Default
    )

    val now = Instant.now()
    val events = (1..3).map { i ->
      LabeledEvent(
        timestamp = now,
        vid = i.toLong(),
        message = createDynamicMessage(TestEvent.newBuilder()
          .setPerson(Person.newBuilder().setAgeGroup(Person.AgeGroup.YEARS_18_TO_34))
          .build()),
        eventGroupReferenceId = "reference-id-1"
      )
    }

    // Create a time interval that includes all events
    val timeInterval = Interval.newBuilder()
      .setStartTime(com.google.protobuf.Timestamp.newBuilder()
        .setSeconds(now.epochSecond - 3600)) // 1 hour before
      .setEndTime(com.google.protobuf.Timestamp.newBuilder()
        .setSeconds(now.epochSecond + 3600)) // 1 hour after
      .build()

    val sinks = listOf(
      FrequencyVectorSink(
        frequencyVector = StripedByteFrequencyVector(TEST_POPULATION_SPEC, TEST_MEASUREMENT_SPEC),
        vidIndexMap = realVidIndexMap,
        filterProcessor = FilterProcessor(
          filterSpec = FilterSpec(
            celExpression = "", // Empty expression matches all events
            collectionInterval = timeInterval,
            eventGroupReferenceIds = listOf("reference-id-1"),
          ),
          eventDescriptor = TestEvent.getDescriptor()
        )
      )
    )

    smallBatchPipeline.processEventBatches(
      eventSource = TestEventSource(listOf(events)),
      sinks = sinks
    )

    assertThat(sinks).hasSize(1)
  }

  @Test
  fun `processEventBatches with multiple workers`() = runBlocking {
    val pipeline = ParallelBatchedPipeline(
      batchSize = 1,
      workers = 4,
      dispatcher = Dispatchers.Default
    )

    val now = Instant.now()
    val events = (1..8).map { i ->
      LabeledEvent(
        timestamp = now,
        vid = i.toLong(),
        message = createDynamicMessage(TestEvent.newBuilder()
          .setPerson(Person.newBuilder().setAgeGroup(Person.AgeGroup.YEARS_18_TO_34))
          .build()),
        eventGroupReferenceId = "reference-id-1"
      )
    }

    // Create a time interval that includes all events
    val timeInterval = Interval.newBuilder()
      .setStartTime(com.google.protobuf.Timestamp.newBuilder()
        .setSeconds(now.epochSecond - 3600)) // 1 hour before
      .setEndTime(com.google.protobuf.Timestamp.newBuilder()
        .setSeconds(now.epochSecond + 3600)) // 1 hour after
      .build()

    val sinks = listOf(
      FrequencyVectorSink(
        frequencyVector = StripedByteFrequencyVector(TEST_POPULATION_SPEC, TEST_MEASUREMENT_SPEC),
        vidIndexMap = realVidIndexMap,
        filterProcessor = FilterProcessor(
          filterSpec = FilterSpec(
            celExpression = "", // Empty expression matches all events
            collectionInterval = timeInterval,
            eventGroupReferenceIds = listOf("reference-id-1"),
          ),
          eventDescriptor = TestEvent.getDescriptor()
        )
      )
    )

    pipeline.processEventBatches(
      eventSource = TestEventSource(listOf(events)),
      sinks = sinks
    )

    assertThat(sinks).hasSize(1)
  }

  companion object {
    private val TEST_POPULATION_SPEC = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 1L
          endVidInclusive = 1000L
        }
      }
    }

    private val TEST_MEASUREMENT_SPEC = measurementSpec {
      vidSamplingInterval = vidSamplingInterval {
        start = 0.0f
        width = 1.0f
      }
      impression = impression {
        maximumFrequencyPerUser = 100
      }
    }
  }
}
