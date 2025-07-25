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
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.DynamicMessage
import com.google.type.Interval
import java.time.Instant
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.subPopulation
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.vidRange
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.InMemoryVidIndexMap
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.VidIndexMap

@RunWith(JUnit4::class)
class ParallelBatchedPipelineTest {

  @get:Rule val tempFolder = TemporaryFolder()

  private val testVidIndexMap: VidIndexMap = InMemoryVidIndexMap.build(TEST_POPULATION_SPEC)

  init {
    AeadConfig.register()
    StreamingAeadConfig.register()
  }

  @Before
  fun setUp() {
    // Set up TypeRegistry
  }

  private fun createDynamicMessage(testEvent: TestEvent): DynamicMessage {
    return DynamicMessage.newBuilder(TestEvent.getDescriptor()).mergeFrom(testEvent).build()
  }

  private class TestEventSource(
    private val eventBatches: List<List<LabeledEvent<out com.google.protobuf.Message>>>
  ) : EventSource {
    override suspend fun generateEventBatches(dispatcher: CoroutineContext): Flow<EventBatch> {
      return eventBatches.asFlow().map { eventList ->
        val events = eventList.map { it as LabeledEvent<com.google.protobuf.Message> }
        // Use the actual event timestamps for minTime and maxTime
        val minTime =
          if (events.isNotEmpty()) {
            events.minOf { it.timestamp }
          } else {
            java.time.Instant.now()
          }
        val maxTime =
          if (events.isNotEmpty()) {
            events.maxOf { it.timestamp }.plusSeconds(1) // Add 1 second to make it exclusive
          } else {
            java.time.Instant.now().plusSeconds(1)
          }
        EventBatch(events = events, minTime = minTime, maxTime = maxTime)
      }
    }
  }

  @Test
  fun `processEventBatches processes empty flow successfully`() = runBlocking {
    val pipeline =
      ParallelBatchedPipeline(batchSize = 10, workers = 2, dispatcher = Dispatchers.Default)

    // Create a wide time interval for testing
    val now = Instant.now()
    val timeInterval =
      Interval.newBuilder()
        .setStartTime(
          com.google.protobuf.Timestamp.newBuilder().setSeconds(now.epochSecond - 86400)
        ) // 1 day before
        .setEndTime(
          com.google.protobuf.Timestamp.newBuilder().setSeconds(now.epochSecond + 86400)
        ) // 1 day after
        .build()

    val sinks =
      listOf(
        FrequencyVectorSink(
          frequencyVector = StripedByteFrequencyVector(testVidIndexMap.size.toInt()),
          vidIndexMap = testVidIndexMap,
          filterProcessor =
            FilterProcessor(
              filterSpec =
                FilterSpec(
                  celExpression = "", // Empty expression matches all events
                  collectionInterval = timeInterval,
                  eventGroupReferenceIds = listOf("reference-id-1"),
                ),
              eventDescriptor = TestEvent.getDescriptor(),
            ),
        )
      )

    pipeline.processEventBatches(eventSource = TestEventSource(emptyList()), sinks = sinks)

    assertThat(sinks).hasSize(1)
    val frequencyVector = sinks.first().getFrequencyVector()
    assertThat(frequencyVector.getReach()).isEqualTo(0L)
    assertThat(frequencyVector.getTotalCount()).isEqualTo(0L)
  }

  @Test
  fun `processEventBatches handles single event`() = runBlocking {
    val pipeline =
      ParallelBatchedPipeline(batchSize = 10, workers = 2, dispatcher = Dispatchers.Default)

    val testEvent =
      TestEvent.newBuilder()
        .setPerson(Person.newBuilder().setAgeGroup(Person.AgeGroup.YEARS_18_TO_34))
        .build()

    val eventTimestamp = Instant.now()
    val labeledEvent =
      LabeledEvent(
        timestamp = eventTimestamp,
        vid = 500L, // Within our TEST_POPULATION_SPEC range (1-1000)
        message = createDynamicMessage(testEvent),
        eventGroupReferenceId = "reference-id-1",
      )

    // Create a time interval that includes the current event
    val timeInterval =
      Interval.newBuilder()
        .setStartTime(
          com.google.protobuf.Timestamp.newBuilder().setSeconds(eventTimestamp.epochSecond - 3600)
        ) // 1 hour before event
        .setEndTime(
          com.google.protobuf.Timestamp.newBuilder().setSeconds(eventTimestamp.epochSecond + 3600)
        ) // 1 hour after event
        .build()

    val sinks =
      listOf(
        FrequencyVectorSink(
          frequencyVector = StripedByteFrequencyVector(testVidIndexMap.size.toInt()),
          vidIndexMap = testVidIndexMap,
          filterProcessor =
            FilterProcessor(
              filterSpec =
                FilterSpec(
                  celExpression = "", // Empty expression matches all events
                  collectionInterval = timeInterval,
                  eventGroupReferenceIds = listOf("reference-id-1"),
                ),
              eventDescriptor = TestEvent.getDescriptor(),
            ),
        )
      )

    pipeline.processEventBatches(
      eventSource = TestEventSource(listOf(listOf(labeledEvent))),
      sinks = sinks,
    )

    assertThat(sinks).hasSize(1)
    val frequencyVector = sinks.first().getFrequencyVector()
    assertThat(frequencyVector.getReach()).isEqualTo(1L)
    assertThat(frequencyVector.getTotalCount()).isEqualTo(1L)
    // Verify the specific VID was counted
    val vidIndex = testVidIndexMap[500L]
    assertThat(frequencyVector.getByteArray()[vidIndex].toInt()).isEqualTo(1)
  }

  @Test
  fun `processEventBatches with time interval filters`() = runBlocking {
    val pipeline =
      ParallelBatchedPipeline(batchSize = 5, workers = 1, dispatcher = Dispatchers.Default)

    val testEvent =
      TestEvent.newBuilder()
        .setPerson(Person.newBuilder().setAgeGroup(Person.AgeGroup.YEARS_18_TO_34))
        .build()

    val now = Instant.now()
    val pastEvent =
      LabeledEvent(
        timestamp = now.minusSeconds(7200), // 2 hours ago
        vid = 600L,
        message = createDynamicMessage(testEvent),
        eventGroupReferenceId = "reference-id-1",
      )
    val currentEvent =
      LabeledEvent(
        timestamp = now.minusSeconds(1800), // 30 minutes ago
        vid = 601L,
        message = createDynamicMessage(testEvent),
        eventGroupReferenceId = "reference-id-1",
      )
    val futureEvent =
      LabeledEvent(
        timestamp = now.plusSeconds(3600), // 1 hour in future
        vid = 602L,
        message = createDynamicMessage(testEvent),
        eventGroupReferenceId = "reference-id-1",
      )

    // Time interval: 1 hour ago to now (should only include currentEvent)
    val timeInterval =
      Interval.newBuilder()
        .setStartTime(
          com.google.protobuf.Timestamp.newBuilder().setSeconds(now.epochSecond - 3600)
        )
        .setEndTime(
          com.google.protobuf.Timestamp.newBuilder().setSeconds(now.epochSecond)
        )
        .build()

    val sinks =
      listOf(
        FrequencyVectorSink(
          frequencyVector = StripedByteFrequencyVector(testVidIndexMap.size.toInt()),
          vidIndexMap = testVidIndexMap,
          filterProcessor =
            FilterProcessor(
              filterSpec =
                FilterSpec(
                  celExpression = "", // Empty expression matches all events
                  collectionInterval = timeInterval,
                  eventGroupReferenceIds = listOf("reference-id-1"),
                ),
              eventDescriptor = TestEvent.getDescriptor(),
            ),
        )
      )

    pipeline.processEventBatches(
      eventSource = TestEventSource(listOf(listOf(pastEvent, currentEvent, futureEvent))),
      sinks = sinks,
    )

    assertThat(sinks).hasSize(1)
    val frequencyVector = sinks.first().getFrequencyVector()
    // Only currentEvent should be included (within time interval)
    assertThat(frequencyVector.getReach()).isEqualTo(1L)
    assertThat(frequencyVector.getTotalCount()).isEqualTo(1L)
    // Verify correct VID was counted
    val currentVidIndex = testVidIndexMap[601L]
    assertThat(frequencyVector.getByteArray()[currentVidIndex].toInt()).isEqualTo(1)
    // Verify other VIDs were not counted
    val pastVidIndex = testVidIndexMap[600L]
    assertThat(frequencyVector.getByteArray()[pastVidIndex].toInt()).isEqualTo(0)
    val futureVidIndex = testVidIndexMap[602L]
    assertThat(frequencyVector.getByteArray()[futureVidIndex].toInt()).isEqualTo(0)
  }

  @Test
  fun `processEventBatches with different batch sizes`() = runBlocking {
    val smallBatchPipeline =
      ParallelBatchedPipeline(batchSize = 2, workers = 1, dispatcher = Dispatchers.Default)

    val now = Instant.now()
    val events =
      (1..5).map { i ->
        LabeledEvent(
          timestamp = now,
          vid = (i * 10).toLong(), // VIDs: 10, 20, 30, 40, 50
          message =
            createDynamicMessage(
              TestEvent.newBuilder()
                .setPerson(Person.newBuilder().setAgeGroup(Person.AgeGroup.YEARS_18_TO_34))
                .build()
            ),
          eventGroupReferenceId = "reference-id-1",
        )
      }

    // Create a time interval that includes all events
    val timeInterval =
      Interval.newBuilder()
        .setStartTime(
          com.google.protobuf.Timestamp.newBuilder().setSeconds(now.epochSecond - 3600)
        ) // 1 hour before
        .setEndTime(
          com.google.protobuf.Timestamp.newBuilder().setSeconds(now.epochSecond + 3600)
        ) // 1 hour after
        .build()

    val sinks =
      listOf(
        FrequencyVectorSink(
          frequencyVector = StripedByteFrequencyVector(testVidIndexMap.size.toInt()),
          vidIndexMap = testVidIndexMap,
          filterProcessor =
            FilterProcessor(
              filterSpec =
                FilterSpec(
                  celExpression = "", // Empty expression matches all events
                  collectionInterval = timeInterval,
                  eventGroupReferenceIds = listOf("reference-id-1"),
                ),
              eventDescriptor = TestEvent.getDescriptor(),
            ),
        )
      )

    smallBatchPipeline.processEventBatches(
      eventSource = TestEventSource(listOf(events)),
      sinks = sinks,
    )

    assertThat(sinks).hasSize(1)
    val frequencyVector = sinks.first().getFrequencyVector()
    // All 5 events should be processed despite batch size of 2
    assertThat(frequencyVector.getReach()).isEqualTo(5L)
    assertThat(frequencyVector.getTotalCount()).isEqualTo(5L)
    // Verify each VID was counted
    for (i in 1..5) {
      val vidIndex = testVidIndexMap[(i * 10).toLong()]
      assertThat(frequencyVector.getByteArray()[vidIndex].toInt()).isEqualTo(1)
    }
  }

  @Test
  fun `processEventBatches with multiple workers`() = runBlocking {
    val pipeline =
      ParallelBatchedPipeline(batchSize = 1, workers = 4, dispatcher = Dispatchers.Default)

    val now = Instant.now()
    // Create events with some duplicate VIDs to test frequency counting
    val events =
      listOf(
        LabeledEvent(timestamp = now, vid = 100L, message = createDynamicMessage(TestEvent.newBuilder().setPerson(Person.newBuilder().setAgeGroup(Person.AgeGroup.YEARS_18_TO_34)).build()), eventGroupReferenceId = "reference-id-1"),
        LabeledEvent(timestamp = now, vid = 100L, message = createDynamicMessage(TestEvent.newBuilder().setPerson(Person.newBuilder().setAgeGroup(Person.AgeGroup.YEARS_18_TO_34)).build()), eventGroupReferenceId = "reference-id-1"),
        LabeledEvent(timestamp = now, vid = 200L, message = createDynamicMessage(TestEvent.newBuilder().setPerson(Person.newBuilder().setAgeGroup(Person.AgeGroup.YEARS_18_TO_34)).build()), eventGroupReferenceId = "reference-id-1"),
        LabeledEvent(timestamp = now, vid = 200L, message = createDynamicMessage(TestEvent.newBuilder().setPerson(Person.newBuilder().setAgeGroup(Person.AgeGroup.YEARS_18_TO_34)).build()), eventGroupReferenceId = "reference-id-1"),
        LabeledEvent(timestamp = now, vid = 200L, message = createDynamicMessage(TestEvent.newBuilder().setPerson(Person.newBuilder().setAgeGroup(Person.AgeGroup.YEARS_18_TO_34)).build()), eventGroupReferenceId = "reference-id-1"),
        LabeledEvent(timestamp = now, vid = 300L, message = createDynamicMessage(TestEvent.newBuilder().setPerson(Person.newBuilder().setAgeGroup(Person.AgeGroup.YEARS_18_TO_34)).build()), eventGroupReferenceId = "reference-id-1"),
        LabeledEvent(timestamp = now, vid = 400L, message = createDynamicMessage(TestEvent.newBuilder().setPerson(Person.newBuilder().setAgeGroup(Person.AgeGroup.YEARS_18_TO_34)).build()), eventGroupReferenceId = "reference-id-1"),
        LabeledEvent(timestamp = now, vid = 500L, message = createDynamicMessage(TestEvent.newBuilder().setPerson(Person.newBuilder().setAgeGroup(Person.AgeGroup.YEARS_18_TO_34)).build()), eventGroupReferenceId = "reference-id-1"),
      )

    // Create a time interval that includes all events
    val timeInterval =
      Interval.newBuilder()
        .setStartTime(
          com.google.protobuf.Timestamp.newBuilder().setSeconds(now.epochSecond - 3600)
        ) // 1 hour before
        .setEndTime(
          com.google.protobuf.Timestamp.newBuilder().setSeconds(now.epochSecond + 3600)
        ) // 1 hour after
        .build()

    val sinks =
      listOf(
        FrequencyVectorSink(
          frequencyVector = StripedByteFrequencyVector(testVidIndexMap.size.toInt()),
          vidIndexMap = testVidIndexMap,
          filterProcessor =
            FilterProcessor(
              filterSpec =
                FilterSpec(
                  celExpression = "", // Empty expression matches all events
                  collectionInterval = timeInterval,
                  eventGroupReferenceIds = listOf("reference-id-1"),
                ),
              eventDescriptor = TestEvent.getDescriptor(),
            ),
        )
      )

    pipeline.processEventBatches(eventSource = TestEventSource(listOf(events)), sinks = sinks)

    assertThat(sinks).hasSize(1)
    val frequencyVector = sinks.first().getFrequencyVector()
    // 5 unique VIDs (100, 200, 300, 400, 500)
    assertThat(frequencyVector.getReach()).isEqualTo(5L)
    // 8 total events
    assertThat(frequencyVector.getTotalCount()).isEqualTo(8L)
    // Verify frequency counts per VID
    val byteArray = frequencyVector.getByteArray()
    assertThat(byteArray[testVidIndexMap[100L]].toInt()).isEqualTo(2)
    assertThat(byteArray[testVidIndexMap[200L]].toInt()).isEqualTo(3)
    assertThat(byteArray[testVidIndexMap[300L]].toInt()).isEqualTo(1)
    assertThat(byteArray[testVidIndexMap[400L]].toInt()).isEqualTo(1)
    assertThat(byteArray[testVidIndexMap[500L]].toInt()).isEqualTo(1)
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

    private fun FrequencyVector.getReach(): Long {
      return getByteArray().count { it > 0 }.toLong()
    }

    private fun FrequencyVector.getTotalCount(): Long {
      return getByteArray().sumOf { it.toInt() }.toLong()
    }
  }
}
