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
import com.google.protobuf.Message
import com.google.protobuf.DynamicMessage
import com.google.protobuf.timestamp
import com.google.type.interval
import java.time.Instant
import java.util.Collections
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.runBlocking
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

  @get:Rule
  val tempFolder = TemporaryFolder()

  private val testVidIndexMap: VidIndexMap = InMemoryVidIndexMap.build(TEST_POPULATION_SPEC)

  init {
    AeadConfig.register()
    StreamingAeadConfig.register()
  }

  private fun createDynamicMessage(testEvent: TestEvent): Message {
    return DynamicMessage.newBuilder(TestEvent.getDescriptor()).mergeFrom(testEvent).build()
  }

  private class TestEventSource(
    private val eventBatches: List<List<LabeledEvent<Message>>>
  ) : EventSource {
    override fun generateEventBatches(): Flow<EventBatch> {
      return eventBatches.asFlow().map { eventList ->
        val events = eventList.map { it as LabeledEvent<Message> }
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
        EventBatch(events = events, minTime = minTime, maxTime = maxTime)
      }
    }
  }

  @Test
  fun `processEventBatches processes empty flow successfully`() = runBlocking {
    val pipeline =
      ParallelBatchedPipeline(batchSize = 10, workers = 2)

    // Create a wide time interval for testing
    val now = Instant.now()
    val timeInterval = interval {
      startTime = timestamp { seconds = now.epochSecond - 86400 }
      endTime = timestamp { seconds = now.epochSecond + 86400 }
    }

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
      ParallelBatchedPipeline(batchSize = 10, workers = 2)

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
      interval {
        startTime = timestamp { seconds = eventTimestamp.epochSecond - 3600 }
        endTime = timestamp { seconds = eventTimestamp.epochSecond + 3600 }
      }

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
      ParallelBatchedPipeline(batchSize = 5, workers = 1)

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
      interval {
        startTime = timestamp { seconds = now.epochSecond - 3600 }
        endTime = timestamp { seconds = now.epochSecond }
      }

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
      ParallelBatchedPipeline(batchSize = 2, workers = 1)

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
      interval {
        startTime = timestamp { seconds = now.epochSecond - 3600 }
        endTime = timestamp { seconds = now.epochSecond + 3600 }
      }

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
      ParallelBatchedPipeline(batchSize = 1, workers = 4)

    val now = Instant.now()
    // Create events with some duplicate VIDs to test frequency counting
    val events =
      listOf(
        LabeledEvent(
          timestamp = now,
          vid = 100L,
          message = createDynamicMessage(
            TestEvent.newBuilder()
              .setPerson(Person.newBuilder().setAgeGroup(Person.AgeGroup.YEARS_18_TO_34)).build()
          ),
          eventGroupReferenceId = "reference-id-1"
        ),
        LabeledEvent(
          timestamp = now,
          vid = 100L,
          message = createDynamicMessage(
            TestEvent.newBuilder()
              .setPerson(Person.newBuilder().setAgeGroup(Person.AgeGroup.YEARS_18_TO_34)).build()
          ),
          eventGroupReferenceId = "reference-id-1"
        ),
        LabeledEvent(
          timestamp = now,
          vid = 200L,
          message = createDynamicMessage(
            TestEvent.newBuilder()
              .setPerson(Person.newBuilder().setAgeGroup(Person.AgeGroup.YEARS_18_TO_34)).build()
          ),
          eventGroupReferenceId = "reference-id-1"
        ),
        LabeledEvent(
          timestamp = now,
          vid = 200L,
          message = createDynamicMessage(
            TestEvent.newBuilder()
              .setPerson(Person.newBuilder().setAgeGroup(Person.AgeGroup.YEARS_18_TO_34)).build()
          ),
          eventGroupReferenceId = "reference-id-1"
        ),
        LabeledEvent(
          timestamp = now,
          vid = 200L,
          message = createDynamicMessage(
            TestEvent.newBuilder()
              .setPerson(Person.newBuilder().setAgeGroup(Person.AgeGroup.YEARS_18_TO_34)).build()
          ),
          eventGroupReferenceId = "reference-id-1"
        ),
        LabeledEvent(
          timestamp = now,
          vid = 300L,
          message = createDynamicMessage(
            TestEvent.newBuilder()
              .setPerson(Person.newBuilder().setAgeGroup(Person.AgeGroup.YEARS_18_TO_34)).build()
          ),
          eventGroupReferenceId = "reference-id-1"
        ),
        LabeledEvent(
          timestamp = now,
          vid = 400L,
          message = createDynamicMessage(
            TestEvent.newBuilder()
              .setPerson(Person.newBuilder().setAgeGroup(Person.AgeGroup.YEARS_18_TO_34)).build()
          ),
          eventGroupReferenceId = "reference-id-1"
        ),
        LabeledEvent(
          timestamp = now,
          vid = 500L,
          message = createDynamicMessage(
            TestEvent.newBuilder()
              .setPerson(Person.newBuilder().setAgeGroup(Person.AgeGroup.YEARS_18_TO_34)).build()
          ),
          eventGroupReferenceId = "reference-id-1"
        ),
      )

    // Create a time interval that includes all events
    val timeInterval =
      interval {
        startTime = timestamp { seconds = now.epochSecond - 3600 }
        endTime = timestamp { seconds = now.epochSecond + 3600 }
      }

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

    private fun StripedByteFrequencyVector.getReach(): Long {
      return getByteArray().count { it > 0 }.toLong()
    }

    private fun StripedByteFrequencyVector.getTotalCount(): Long {
      return getByteArray().sumOf { it.toInt() }.toLong()
    }
  }

  @Test
  fun `multiple workers process batches concurrently`() = runBlocking {
    val pipeline = ParallelBatchedPipeline(batchSize = 1, workers = 3)

    val now = Instant.now()
    val events = (1..6).map { i ->
      LabeledEvent(
        timestamp = now,
        vid = i.toLong(),
        message = DynamicMessage.getDefaultInstance(TestEvent.getDescriptor()) as Message,
        eventGroupReferenceId = "reference-id-1",
      )
    }

    // Track which worker processed each batch
    val workerProcessingLog = Collections.synchronizedList(mutableListOf<String>())

    class WorkerTrackingSink(
      filterProcessor: FilterProcessor,
      frequencyVector: StripedByteFrequencyVector,
      vidIndexMap: VidIndexMap
    ) : FrequencyVectorSink(filterProcessor, frequencyVector, vidIndexMap) {
      override fun processBatch(batch: EventBatch) {
        val workerName = Thread.currentThread().name
        batch.events.forEach { event ->
          workerProcessingLog.add("Worker $workerName processed VID ${event.vid}")
        }
        super.processBatch(batch)
      }
    }

    val sinks = listOf(
      WorkerTrackingSink(
        filterProcessor = FilterProcessor(
          filterSpec = FilterSpec(
            celExpression = "",
            collectionInterval = interval {
              startTime = timestamp { seconds = now.epochSecond - 3600 }
              endTime = timestamp { seconds = now.epochSecond + 3600 }
            },
            eventGroupReferenceIds = listOf("reference-id-1")
          ),
          eventDescriptor = TestEvent.getDescriptor()
        ),
        frequencyVector = StripedByteFrequencyVector(testVidIndexMap.size.toInt()),
        vidIndexMap = testVidIndexMap
      )
    )

    // Create multiple batches to ensure round-robin distribution across workers
    val eventBatches = events.chunked(2) // Split into 3 batches of 2 events each
    pipeline.processEventBatches(
      eventSource = TestEventSource(eventBatches.map { it }),
      sinks = sinks
    )

    // Verify all events were processed
    assertThat(workerProcessingLog).hasSize(6)

    // Verify events were distributed (may be processed by same or different workers)
    val uniqueWorkers = workerProcessingLog.map { it.split(" ")[1] }.toSet()
    assertThat(uniqueWorkers.size).isAtLeast(1)

    // Verify all VIDs were processed
    val processedVids = workerProcessingLog.map { it.split(" ").last() }.sorted()
    assertThat(processedVids).containsExactly("1", "2", "3", "4", "5", "6").inOrder()
  }

  @Test
  fun `source failure cancels pipeline early`() = runBlocking {
    // Pipeline with small batches, multiple workers to exercise fan-out
    val pipeline = ParallelBatchedPipeline(batchSize = 1, workers = 2)

    // Source that throws after a few emissions
    val now = Instant.now()
    val throwAfter = 5
    var emitted = 0
    val source = object : EventSource {
      override fun generateEventBatches(): Flow<EventBatch> = flow {
        repeat(100) { i ->
          if (emitted >= throwAfter) {
            throw IllegalStateException("source failure")
          }
          emitted++
          val e = LabeledEvent(
            timestamp = now,
            vid = (i + 1).toLong(),
            message = DynamicMessage.getDefaultInstance(TestEvent.getDescriptor()) as Message,
            eventGroupReferenceId = "reference-id-1",
          )
          emit(EventBatch(events = listOf(e), minTime = now, maxTime = now.plusSeconds(1)))
        }
      }
    }

    val sinks = listOf(
      FrequencyVectorSink(
        filterProcessor = FilterProcessor(
          filterSpec = FilterSpec(
            celExpression = "",
            collectionInterval = interval {
              startTime = timestamp { seconds = now.epochSecond - 3600 }
              endTime = timestamp { seconds = now.epochSecond + 3600 }
            },
            eventGroupReferenceIds = listOf("reference-id-1")
          ),
          eventDescriptor = TestEvent.getDescriptor()
        ),
        frequencyVector = StripedByteFrequencyVector(1024),
        vidIndexMap = InMemoryVidIndexMap.build(TEST_POPULATION_SPEC)
      )
    )

    val ex = kotlin.runCatching {
      pipeline.processEventBatches(source, sinks)
    }.exceptionOrNull()

    // Must fail with the source exception
    assertThat(ex).isInstanceOf(IllegalStateException::class.java)
    assertThat(ex?.message).isEqualTo("source failure")

    // Should have emitted exactly throwAfter batches
    assertThat(emitted).isEqualTo(throwAfter)
  }

  @Test
  fun `processEventBatches with multiple sinks processes all sinks per batch`() = runBlocking {
    val pipeline = ParallelBatchedPipeline(batchSize = 2, workers = 1)

    val now = Instant.now()
    val events = (1..4).map { i ->
      LabeledEvent(
        timestamp = now,
        vid = (i * 10).toLong(),
        message = createDynamicMessage(
          TestEvent.newBuilder()
            .setPerson(Person.newBuilder().setAgeGroup(Person.AgeGroup.YEARS_18_TO_34))
            .build()
        ),
        eventGroupReferenceId = "reference-id-1",
      )
    }

    // Track which sinks processed which batches
    val sink1Processed = mutableListOf<Int>()
    val sink2Processed = mutableListOf<Int>()

    class TrackingFrequencyVectorSink(
      private val sinkId: Int,
      private val processedList: MutableList<Int>,
      filterProcessor: FilterProcessor,
      frequencyVector: StripedByteFrequencyVector,
      vidIndexMap: VidIndexMap
    ) : FrequencyVectorSink(filterProcessor, frequencyVector, vidIndexMap) {
      override fun processBatch(batch: EventBatch) {
        // Record batch VID to identify which batch was processed
        batch.events.forEach { event ->
          processedList.add((event.vid / 10).toInt())
        }
        super.processBatch(batch)
      }
    }

    val sinks = listOf(
      TrackingFrequencyVectorSink(
        sinkId = 1,
        processedList = sink1Processed,
        filterProcessor = FilterProcessor(
          filterSpec = FilterSpec(
            celExpression = "",
            collectionInterval = interval {
              startTime = timestamp { seconds = now.epochSecond - 3600 }
              endTime = timestamp { seconds = now.epochSecond + 3600 }
            },
            eventGroupReferenceIds = listOf("reference-id-1")
          ),
          eventDescriptor = TestEvent.getDescriptor()
        ),
        frequencyVector = StripedByteFrequencyVector(testVidIndexMap.size.toInt()),
        vidIndexMap = testVidIndexMap
      ),
      TrackingFrequencyVectorSink(
        sinkId = 2,
        processedList = sink2Processed,
        filterProcessor = FilterProcessor(
          filterSpec = FilterSpec(
            celExpression = "person.age_group == 1",  // YEARS_18_TO_34 enum value
            collectionInterval = interval {
              startTime = timestamp { seconds = now.epochSecond - 3600 }
              endTime = timestamp { seconds = now.epochSecond + 3600 }
            },
            eventGroupReferenceIds = listOf("reference-id-1")
          ),
          eventDescriptor = TestEvent.getDescriptor()
        ),
        frequencyVector = StripedByteFrequencyVector(testVidIndexMap.size.toInt()),
        vidIndexMap = testVidIndexMap
      )
    )

    pipeline.processEventBatches(
      eventSource = TestEventSource(listOf(events)),
      sinks = sinks
    )

    // Both sinks should have processed all events
    assertThat(sink1Processed).containsExactly(1, 2, 3, 4)
    assertThat(sink2Processed).containsExactly(1, 2, 3, 4)

    // Verify frequency vectors
    val fv1 = sinks[0].getFrequencyVector()
    val fv2 = sinks[1].getFrequencyVector()
    assertThat(fv1.getReach()).isEqualTo(4L)
    assertThat(fv2.getReach()).isEqualTo(4L)
  }

  @Test
  fun `channel backpressure controls batch flow`() = runBlocking {
    // Small channel capacity and slow processing to test backpressure
    val pipeline = ParallelBatchedPipeline(batchSize = 1, workers = 1, workerChannelCapacity = 2)

    val now = Instant.now()
    val totalBatches = 10
    val batchEmissionOrder = Collections.synchronizedList(mutableListOf<Int>())
    val batchProcessingOrder = Collections.synchronizedList(mutableListOf<Int>())

    // Source that tracks emission order and includes processing delays
    val source = object : EventSource {
      override fun generateEventBatches(): Flow<EventBatch> = flow {
        repeat(totalBatches) { i ->
          val batchId = i + 1
          batchEmissionOrder.add(batchId)
          val e = LabeledEvent(
            timestamp = now,
            vid = batchId.toLong(),
            message = DynamicMessage.getDefaultInstance(TestEvent.getDescriptor()) as Message,
            eventGroupReferenceId = "reference-id-1",
          )
          emit(EventBatch(events = listOf(e), minTime = now, maxTime = now.plusSeconds(1)))
        }
      }
    }

    // Slow sink that tracks processing order
    class SlowProcessingSink(
      filterProcessor: FilterProcessor,
      frequencyVector: StripedByteFrequencyVector,
      vidIndexMap: VidIndexMap
    ) : FrequencyVectorSink(filterProcessor, frequencyVector, vidIndexMap) {
      override fun processBatch(batch: EventBatch) {
        val batchId = batch.events.first().vid.toInt()
        batchProcessingOrder.add(batchId)
        Thread.sleep(100) // Slow processing to test backpressure
        super.processBatch(batch)
      }
    }

    val sinks = listOf(
      SlowProcessingSink(
        filterProcessor = FilterProcessor(
          filterSpec = FilterSpec(
            celExpression = "",
            collectionInterval = interval {
              startTime = timestamp { seconds = now.epochSecond - 3600 }
              endTime = timestamp { seconds = now.epochSecond + 3600 }
            },
            eventGroupReferenceIds = listOf("reference-id-1")
          ),
          eventDescriptor = TestEvent.getDescriptor()
        ),
        frequencyVector = StripedByteFrequencyVector(testVidIndexMap.size.toInt()),
        vidIndexMap = testVidIndexMap
      )
    )

    pipeline.processEventBatches(source, sinks)

    // Verify all batches were emitted and processed
    assertThat(batchEmissionOrder).hasSize(totalBatches)
    assertThat(batchProcessingOrder).hasSize(totalBatches)

    // Verify processing order matches emission order (FIFO due to single worker)
    assertThat(batchProcessingOrder).containsExactlyElementsIn(batchEmissionOrder).inOrder()

    // All batches should be processed despite backpressure
    assertThat(batchProcessingOrder).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).inOrder()
  }

  @Test
  fun `worker failure cancels pipeline and propagates exception`() = runBlocking {
    val pipeline = ParallelBatchedPipeline(batchSize = 1, workers = 2)

    val now = Instant.now()
    var emitted = 0
    val source = object : EventSource {
      override fun generateEventBatches(): Flow<EventBatch> = flow {
        repeat(100) { i ->
          emitted++
          val e = LabeledEvent(
            timestamp = now,
            vid = (i + 1).toLong(),
            message = DynamicMessage.getDefaultInstance(TestEvent.getDescriptor()) as Message,
            eventGroupReferenceId = "reference-id-1",
          )
          emit(EventBatch(events = listOf(e), minTime = now, maxTime = now.plusSeconds(1)))
          kotlinx.coroutines.yield() // Allow cancellation
        }
      }
    }

    // Sink that throws on the third batch
    class FailingFrequencyVectorSink(
      filterProcessor: FilterProcessor,
      frequencyVector: StripedByteFrequencyVector,
      vidIndexMap: VidIndexMap
    ) : FrequencyVectorSink(filterProcessor, frequencyVector, vidIndexMap) {
      private var processedCount = 0

      override fun processBatch(batch: EventBatch) {
        processedCount++
        if (processedCount == 3) {
          throw IllegalStateException("worker failure at batch 3")
        }
        super.processBatch(batch)
      }
    }

    val sinks = listOf(
      FailingFrequencyVectorSink(
        filterProcessor = FilterProcessor(
          filterSpec = FilterSpec(
            celExpression = "",
            collectionInterval = interval {
              startTime = timestamp { seconds = now.epochSecond - 3600 }
              endTime = timestamp { seconds = now.epochSecond + 3600 }
            },
            eventGroupReferenceIds = listOf("reference-id-1")
          ),
          eventDescriptor = TestEvent.getDescriptor()
        ),
        frequencyVector = StripedByteFrequencyVector(1024),
        vidIndexMap = InMemoryVidIndexMap.build(TEST_POPULATION_SPEC)
      )
    )

    val ex = kotlin.runCatching {
      pipeline.processEventBatches(source, sinks)
    }.exceptionOrNull()

    // Must fail with worker exception
    assertThat(ex).isInstanceOf(IllegalStateException::class.java)
    assertThat(ex?.message).isEqualTo("worker failure at batch 3")

    // Pipeline should have stopped early (not processed all 100 batches)
    assertThat(emitted).isLessThan(100)
  }
}
