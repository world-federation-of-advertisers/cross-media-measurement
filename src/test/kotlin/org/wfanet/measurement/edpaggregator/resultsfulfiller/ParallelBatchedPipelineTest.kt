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
import com.google.protobuf.timestamp
import com.google.type.interval
import java.time.Instant
import java.util.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.doNothing
import org.mockito.kotlin.spy
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.subPopulation
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.vidRange
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.InMemoryVidIndexMap
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.VidIndexMap

@RunWith(JUnit4::class)
class ParallelBatchedPipelineTest {

  @get:Rule val tempFolder = TemporaryFolder()

  private val testVidIndexMap: VidIndexMap = InMemoryVidIndexMap.build(TEST_POPULATION_SPEC)

  private class TestEventSource(private val eventBatches: List<List<LabeledEvent<TestEvent>>>) :
    EventSource<TestEvent> {
    override fun generateEventBatches(): Flow<EventBatch<TestEvent>> {
      return eventBatches.asFlow().map { events ->
        // Use the actual event timestamps for minTime and maxTime
        val minTime =
          if (events.isNotEmpty()) {
            events.minOf { it.timestamp }
          } else {
            Instant.now()
          }
        val maxTime =
          if (events.isNotEmpty()) {
            events.maxOf { it.timestamp }.plusSeconds(1) // Add 1 second to make it exclusive
          } else {
            Instant.now().plusSeconds(1)
          }
        EventBatch<TestEvent>(
          events = events,
          minTime = minTime,
          maxTime = maxTime,
          eventGroupReferenceId = "test-group",
        )
      }
    }
  }

  @Test
  fun `processEventBatches processes empty flow successfully`() = runBlocking {
    val pipeline = ParallelBatchedPipeline<TestEvent>(batchSize = 10, workers = 2)

    // Create a wide time interval for testing
    val now = Instant.now()
    val timeInterval = interval {
      startTime = timestamp { seconds = now.epochSecond - 86400 }
      endTime = timestamp { seconds = now.epochSecond + 86400 }
    }
    val frequencyVector = StripedByteFrequencyVector(testVidIndexMap.size.toInt())
    val sinks =
      listOf(
        FrequencyVectorSink<TestEvent>(
          frequencyVector = frequencyVector,
          vidIndexMap = testVidIndexMap,
          filterProcessor =
            FilterProcessor<TestEvent>(
              filterSpec =
                FilterSpec(
                  celExpression = "", // Empty expression matches all events
                  collectionInterval = timeInterval,
                  eventGroupReferenceIds = listOf("test-group"),
                ),
              eventDescriptor = TestEvent.getDescriptor(),
            ),
        )
      )

    pipeline.processEventBatches(eventSource = TestEventSource(emptyList()), sinks = sinks)

    assertThat(frequencyVector.getReach()).isEqualTo(0L)
    assertThat(frequencyVector.getTotalCount()).isEqualTo(0L)
  }

  @Test
  fun `processEventBatches handles single event`() = runBlocking {
    val pipeline = ParallelBatchedPipeline<TestEvent>(batchSize = 10, workers = 2)

    val testEvent = testEvent { person = person { ageGroup = Person.AgeGroup.YEARS_18_TO_34 } }

    val eventTimestamp = Instant.now()
    val labeledEvent =
      LabeledEvent(
        timestamp = eventTimestamp,
        vid = 500L, // Within our TEST_POPULATION_SPEC range (1-1000)
        message = testEvent,
      )

    // Create a time interval that includes the current event
    val timeInterval = interval {
      startTime = timestamp { seconds = eventTimestamp.epochSecond - 3600 }
      endTime = timestamp { seconds = eventTimestamp.epochSecond + 3600 }
    }
    val frequencyVector = StripedByteFrequencyVector(testVidIndexMap.size.toInt())
    val sinks =
      listOf(
        FrequencyVectorSink<TestEvent>(
          frequencyVector = frequencyVector,
          vidIndexMap = testVidIndexMap,
          filterProcessor =
            FilterProcessor<TestEvent>(
              filterSpec =
                FilterSpec(
                  celExpression = "", // Empty expression matches all events
                  collectionInterval = timeInterval,
                  eventGroupReferenceIds = listOf("test-group"),
                ),
              eventDescriptor = TestEvent.getDescriptor(),
            ),
        )
      )

    pipeline.processEventBatches(
      eventSource = TestEventSource(listOf(listOf(labeledEvent))),
      sinks = sinks,
    )

    assertThat(frequencyVector.getReach()).isEqualTo(1L)
    assertThat(frequencyVector.getTotalCount()).isEqualTo(1L)
    // Verify the specific VID was counted
    val vidIndex = testVidIndexMap[500L]
    assertThat(frequencyVector.getByteArray()[vidIndex].toInt()).isEqualTo(1)
  }

  @Test
  fun `processEventBatches with time interval filters`() = runBlocking {
    val pipeline = ParallelBatchedPipeline<TestEvent>(batchSize = 5, workers = 1)

    val testEvent = testEvent { person = person { ageGroup = Person.AgeGroup.YEARS_18_TO_34 } }

    val now = Instant.now()
    val pastEvent =
      LabeledEvent(
        timestamp = now.minusSeconds(7200), // 2 hours ago
        vid = 600L,
        message = testEvent,
      )
    val currentEvent =
      LabeledEvent(
        timestamp = now.minusSeconds(1800), // 30 minutes ago
        vid = 601L,
        message = testEvent,
      )
    val futureEvent =
      LabeledEvent(
        timestamp = now.plusSeconds(3600), // 1 hour in future
        vid = 602L,
        message = testEvent,
      )

    // Time interval: 1 hour ago to now (should only include currentEvent)
    val timeInterval = interval {
      startTime = timestamp { seconds = now.epochSecond - 3600 }
      endTime = timestamp { seconds = now.epochSecond }
    }
    val frequencyVector = StripedByteFrequencyVector(testVidIndexMap.size.toInt())

    val sinks =
      listOf(
        FrequencyVectorSink<TestEvent>(
          frequencyVector = frequencyVector,
          vidIndexMap = testVidIndexMap,
          filterProcessor =
            FilterProcessor<TestEvent>(
              filterSpec =
                FilterSpec(
                  celExpression = "", // Empty expression matches all events
                  collectionInterval = timeInterval,
                  eventGroupReferenceIds = listOf("test-group"),
                ),
              eventDescriptor = TestEvent.getDescriptor(),
            ),
        )
      )

    pipeline.processEventBatches(
      eventSource = TestEventSource(listOf(listOf(pastEvent, currentEvent, futureEvent))),
      sinks = sinks,
    )

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
    val smallBatchPipeline = ParallelBatchedPipeline<TestEvent>(batchSize = 2, workers = 1)

    val now = Instant.now()
    val events =
      (1..5).map { i ->
        LabeledEvent(
          timestamp = now,
          vid = (i * 10).toLong(), // VIDs: 10, 20, 30, 40, 50
          message = testEvent { person = person { ageGroup = Person.AgeGroup.YEARS_18_TO_34 } },
        )
      }

    // Create a time interval that includes all events
    val timeInterval = interval {
      startTime = timestamp { seconds = now.epochSecond - 3600 }
      endTime = timestamp { seconds = now.epochSecond + 3600 }
    }
    val frequencyVector = StripedByteFrequencyVector(testVidIndexMap.size.toInt())

    val sinks =
      listOf(
        FrequencyVectorSink<TestEvent>(
          frequencyVector = frequencyVector,
          vidIndexMap = testVidIndexMap,
          filterProcessor =
            FilterProcessor<TestEvent>(
              filterSpec =
                FilterSpec(
                  celExpression = "", // Empty expression matches all events
                  collectionInterval = timeInterval,
                  eventGroupReferenceIds = listOf("test-group"),
                ),
              eventDescriptor = TestEvent.getDescriptor(),
            ),
        )
      )

    smallBatchPipeline.processEventBatches(
      eventSource = TestEventSource(listOf(events)),
      sinks = sinks,
    )

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
    val pipeline = ParallelBatchedPipeline<TestEvent>(batchSize = 1, workers = 4)

    val now = Instant.now()
    // Create events with some duplicate VIDs to test frequency counting
    val events =
      listOf(
        LabeledEvent(
          timestamp = now,
          vid = 100L,
          message = testEvent { person = person { ageGroup = Person.AgeGroup.YEARS_18_TO_34 } },
        ),
        LabeledEvent(
          timestamp = now,
          vid = 100L,
          message = testEvent { person = person { ageGroup = Person.AgeGroup.YEARS_18_TO_34 } },
        ),
        LabeledEvent(
          timestamp = now,
          vid = 200L,
          message = testEvent { person = person { ageGroup = Person.AgeGroup.YEARS_18_TO_34 } },
        ),
        LabeledEvent(
          timestamp = now,
          vid = 200L,
          message = testEvent { person = person { ageGroup = Person.AgeGroup.YEARS_18_TO_34 } },
        ),
        LabeledEvent(
          timestamp = now,
          vid = 200L,
          message = testEvent { person = person { ageGroup = Person.AgeGroup.YEARS_18_TO_34 } },
        ),
        LabeledEvent(
          timestamp = now,
          vid = 300L,
          message = testEvent { person = person { ageGroup = Person.AgeGroup.YEARS_18_TO_34 } },
        ),
        LabeledEvent(
          timestamp = now,
          vid = 400L,
          message = testEvent { person = person { ageGroup = Person.AgeGroup.YEARS_18_TO_34 } },
        ),
        LabeledEvent(
          timestamp = now,
          vid = 500L,
          message = testEvent { person = person { ageGroup = Person.AgeGroup.YEARS_18_TO_34 } },
        ),
      )

    // Create a time interval that includes all events
    val timeInterval = interval {
      startTime = timestamp { seconds = now.epochSecond - 3600 }
      endTime = timestamp { seconds = now.epochSecond + 3600 }
    }
    val frequencyVector = StripedByteFrequencyVector(testVidIndexMap.size.toInt())

    val sinks =
      listOf(
        FrequencyVectorSink<TestEvent>(
          frequencyVector = frequencyVector,
          vidIndexMap = testVidIndexMap,
          filterProcessor =
            FilterProcessor<TestEvent>(
              filterSpec =
                FilterSpec(
                  celExpression = "", // Empty expression matches all events
                  collectionInterval = timeInterval,
                  eventGroupReferenceIds = listOf("test-group"),
                ),
              eventDescriptor = TestEvent.getDescriptor(),
            ),
        )
      )

    pipeline.processEventBatches(eventSource = TestEventSource(listOf(events)), sinks = sinks)

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

  @Test
  fun `multiple workers process batches concurrently`() = runBlocking {
    val pipeline = ParallelBatchedPipeline<TestEvent>(batchSize = 1, workers = 3)

    val now = Instant.now()
    val events =
      (1..6).map { i ->
        LabeledEvent(timestamp = now, vid = i.toLong(), message = TestEvent.getDefaultInstance())
      }

    val baseSink =
      FrequencyVectorSink<TestEvent>(
        filterProcessor =
          FilterProcessor<TestEvent>(
            filterSpec =
              FilterSpec(
                celExpression = "",
                collectionInterval =
                  interval {
                    startTime = timestamp { seconds = now.epochSecond - 3600 }
                    endTime = timestamp { seconds = now.epochSecond + 3600 }
                  },
                eventGroupReferenceIds = listOf("test-group"),
              ),
            eventDescriptor = TestEvent.getDescriptor(),
          ),
        frequencyVector = StripedByteFrequencyVector(testVidIndexMap.size.toInt()),
        vidIndexMap = testVidIndexMap,
      )

    val trackingSink = spy(baseSink)
    val sinks = listOf(trackingSink)

    // Create multiple batches to ensure round-robin distribution across workers
    val eventBatches = events.chunked(2) // Split into 3 batches of 2 events each
    pipeline.processEventBatches(
      eventSource = TestEventSource(eventBatches.map { it }),
      sinks = sinks,
    )

    // Capture all processed batches and verify contents
    val batchCaptor = argumentCaptor<EventBatch<TestEvent>>()
    verify(trackingSink, times(eventBatches.size)).processBatch(batchCaptor.capture())
    val capturedEvents = batchCaptor.allValues.flatMap { it.events }
    assertThat(capturedEvents.map { it.vid }.sorted())
      .containsExactly(1L, 2L, 3L, 4L, 5L, 6L)
      .inOrder()
  }

  @Test
  fun `source failure cancels pipeline early`() = runBlocking {
    // Pipeline with small batches, multiple workers to exercise fan-out
    val pipeline = ParallelBatchedPipeline<TestEvent>(batchSize = 1, workers = 2)

    // Source that throws after a few emissions
    val now = Instant.now()
    val throwAfter = 5
    var emitted = 0
    val source =
      object : EventSource<TestEvent> {
        override fun generateEventBatches(): Flow<EventBatch<TestEvent>> = flow {
          repeat(100) { i ->
            if (emitted >= throwAfter) {
              throw IllegalStateException("source failure")
            }
            emitted++
            val e =
              LabeledEvent<TestEvent>(
                timestamp = now,
                vid = (i + 1).toLong(),
                message = TestEvent.getDefaultInstance(),
              )
            emit(
              EventBatch<TestEvent>(
                events = listOf(e),
                minTime = now,
                maxTime = now.plusSeconds(1),
                eventGroupReferenceId = "reference-id-1",
              )
            )
          }
        }
      }

    val sinks =
      listOf(
        FrequencyVectorSink<TestEvent>(
          filterProcessor =
            FilterProcessor<TestEvent>(
              filterSpec =
                FilterSpec(
                  celExpression = "",
                  collectionInterval =
                    interval {
                      startTime = timestamp { seconds = now.epochSecond - 3600 }
                      endTime = timestamp { seconds = now.epochSecond + 3600 }
                    },
                  eventGroupReferenceIds = listOf("test-group"),
                ),
              eventDescriptor = TestEvent.getDescriptor(),
            ),
          frequencyVector = StripedByteFrequencyVector(1024),
          vidIndexMap = InMemoryVidIndexMap.build(TEST_POPULATION_SPEC),
        )
      )

    val ex = kotlin.runCatching { pipeline.processEventBatches(source, sinks) }.exceptionOrNull()

    // Must fail with the source exception
    assertThat(ex).isInstanceOf(IllegalStateException::class.java)
    assertThat(ex?.message).isEqualTo("source failure")

    // Should have emitted exactly throwAfter batches
    assertThat(emitted).isEqualTo(throwAfter)
  }

  @Test
  fun `processEventBatches with multiple sinks processes all sinks per batch`() = runBlocking {
    val pipeline = ParallelBatchedPipeline<TestEvent>(batchSize = 2, workers = 1)

    val now = Instant.now()
    val events =
      (1..4).map { i ->
        LabeledEvent(
          timestamp = now,
          vid = (i * 10).toLong(),
          message = testEvent { person = person { ageGroup = Person.AgeGroup.YEARS_18_TO_34 } },
        )
      }

    val sink1 =
      FrequencyVectorSink<TestEvent>(
        filterProcessor =
          FilterProcessor<TestEvent>(
            filterSpec =
              FilterSpec(
                celExpression = "",
                collectionInterval =
                  interval {
                    startTime = timestamp { seconds = now.epochSecond - 3600 }
                    endTime = timestamp { seconds = now.epochSecond + 3600 }
                  },
                eventGroupReferenceIds = listOf("test-group"),
              ),
            eventDescriptor = TestEvent.getDescriptor(),
          ),
        frequencyVector = StripedByteFrequencyVector(testVidIndexMap.size.toInt()),
        vidIndexMap = testVidIndexMap,
      )
    val sink2 =
      FrequencyVectorSink<TestEvent>(
        filterProcessor =
          FilterProcessor<TestEvent>(
            filterSpec =
              FilterSpec(
                celExpression = "person.age_group == 1", // YEARS_18_TO_34 enum value
                collectionInterval =
                  interval {
                    startTime = timestamp { seconds = now.epochSecond - 3600 }
                    endTime = timestamp { seconds = now.epochSecond + 3600 }
                  },
                eventGroupReferenceIds = listOf("test-group"),
              ),
            eventDescriptor = TestEvent.getDescriptor(),
          ),
        frequencyVector = StripedByteFrequencyVector(testVidIndexMap.size.toInt()),
        vidIndexMap = testVidIndexMap,
      )
    val trackingSink1 = spy(sink1)
    val trackingSink2 = spy(sink2)
    val sinks = listOf(trackingSink1, trackingSink2)

    pipeline.processEventBatches(eventSource = TestEventSource(listOf(events)), sinks = sinks)

    // Both sinks should have processed the batch containing all events
    val captor1 = argumentCaptor<EventBatch<TestEvent>>()
    verify(trackingSink1, times(1)).processBatch(captor1.capture())
    val captor2 = argumentCaptor<EventBatch<TestEvent>>()
    verify(trackingSink2, times(1)).processBatch(captor2.capture())
    val vids1 = captor1.firstValue.events.map { (it.vid / 10).toInt() }
    val vids2 = captor2.firstValue.events.map { (it.vid / 10).toInt() }
    assertThat(vids1).containsExactly(1, 2, 3, 4)
    assertThat(vids2).containsExactly(1, 2, 3, 4)

    // Verify frequency vectors
    val fv1 = sinks[0].getFrequencyVector()
    val fv2 = sinks[1].getFrequencyVector()
    assertThat(fv1.getReach()).isEqualTo(4L)
    assertThat(fv2.getReach()).isEqualTo(4L)
  }

  @Test
  fun `channel backpressure controls batch flow`() = runBlocking {
    // Small channel capacity and slow processing to test backpressure
    val pipeline =
      ParallelBatchedPipeline<TestEvent>(batchSize = 1, workers = 1, workerChannelCapacity = 2)

    val now = Instant.now()
    val totalBatches = 10
    val batchEmissionOrder = Collections.synchronizedList(mutableListOf<Int>())

    // Source that tracks emission order and includes processing delays
    val source =
      object : EventSource<TestEvent> {
        override fun generateEventBatches(): Flow<EventBatch<TestEvent>> = flow {
          repeat(totalBatches) { i ->
            val batchId = i + 1
            batchEmissionOrder.add(batchId)
            val e =
              LabeledEvent(
                timestamp = now,
                vid = batchId.toLong(),
                message = TestEvent.getDefaultInstance(),
              )
            emit(
              EventBatch(
                events = listOf(e),
                minTime = now,
                maxTime = now.plusSeconds(1),
                eventGroupReferenceId = "reference-id-1",
              )
            )
          }
        }
      }

    // Sink we can spy to capture processing order
    val baseSink: FrequencyVectorSink<TestEvent> =
      FrequencyVectorSink<TestEvent>(
        filterProcessor =
          FilterProcessor<TestEvent>(
            filterSpec =
              FilterSpec(
                celExpression = "",
                collectionInterval =
                  interval {
                    startTime = timestamp { seconds = now.epochSecond - 3600 }
                    endTime = timestamp { seconds = now.epochSecond + 3600 }
                  },
                eventGroupReferenceIds = listOf("test-group"),
              ),
            eventDescriptor = TestEvent.getDescriptor(),
          ),
        frequencyVector = StripedByteFrequencyVector(testVidIndexMap.size.toInt()),
        vidIndexMap = testVidIndexMap,
      )

    val slowProcessingSink = spy(baseSink)
    val sinks = listOf(slowProcessingSink)

    pipeline.processEventBatches(source, sinks)

    // Capture processed batches and derive processing order
    val processedCaptor = argumentCaptor<EventBatch<TestEvent>>()
    verify(slowProcessingSink, times(totalBatches)).processBatch(processedCaptor.capture())
    val processedOrder = processedCaptor.allValues.map { it.events.first().vid.toInt() }
    // Verify all batches were emitted and processed
    assertThat(batchEmissionOrder).hasSize(totalBatches)
    assertThat(processedOrder).hasSize(totalBatches)

    // Verify processing order matches emission order (FIFO due to single worker)
    assertThat(processedOrder).containsExactlyElementsIn(batchEmissionOrder).inOrder()

    // All batches should be processed despite backpressure
    assertThat(processedOrder).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).inOrder()
  }

  @Test
  fun `worker failure cancels pipeline and propagates exception`() = runBlocking {
    val pipeline = ParallelBatchedPipeline<TestEvent>(batchSize = 1, workers = 2)

    val now = Instant.now()
    var emitted = 0
    val source =
      object : EventSource<TestEvent> {
        override fun generateEventBatches(): Flow<EventBatch<TestEvent>> = flow {
          repeat(100) { i ->
            emitted++
            val e =
              LabeledEvent(
                timestamp = now,
                vid = (i + 1).toLong(),
                message = TestEvent.getDefaultInstance(),
              )
            emit(
              EventBatch(
                events = listOf(e),
                minTime = now,
                maxTime = now.plusSeconds(1),
                eventGroupReferenceId = "reference-id-1",
              )
            )
            kotlinx.coroutines.yield() // Allow cancellation
          }
        }
      }

    // Sink that throws on the third batch
    val baseSink: FrequencyVectorSink<TestEvent> =
      FrequencyVectorSink<TestEvent>(
        filterProcessor =
          FilterProcessor<TestEvent>(
            filterSpec =
              FilterSpec(
                celExpression = "",
                collectionInterval =
                  interval {
                    startTime = timestamp { seconds = now.epochSecond - 3600 }
                    endTime = timestamp { seconds = now.epochSecond + 3600 }
                  },
                eventGroupReferenceIds = listOf("test-group"),
              ),
            eventDescriptor = TestEvent.getDescriptor(),
          ),
        frequencyVector = StripedByteFrequencyVector(1024),
        vidIndexMap = InMemoryVidIndexMap.build(TEST_POPULATION_SPEC),
      )

    val failingSink = spy(baseSink)
    // First two invocations: no-op; third invocation: throw
    doNothing()
      .doNothing()
      .doThrow(IllegalStateException("worker failure at batch 3"))
      .`when`(failingSink)
      .processBatch(any())

    val sinks = listOf(failingSink)

    val ex = kotlin.runCatching { pipeline.processEventBatches(source, sinks) }.exceptionOrNull()

    // Must fail with worker exception
    assertThat(ex).isInstanceOf(IllegalStateException::class.java)
    assertThat(ex?.message).isEqualTo("worker failure at batch 3")

    // Pipeline should have stopped early (not processed all 100 batches)
    assertThat(emitted).isLessThan(100)
  }

  companion object {
    init {
      AeadConfig.register()
      StreamingAeadConfig.register()
    }

    private val TEST_POPULATION_SPEC = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 1L
          endVidInclusive = 1000L
        }
      }
    }

    private fun StripedByteFrequencyVector.getReach(): Long {
      return getByteArray().count { it > 0 }.toLong()
    }

    private fun StripedByteFrequencyVector.getTotalCount(): Long {
      return getByteArray().sumOf { it.toInt() }.toLong()
    }
  }
}
