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
import com.google.protobuf.DynamicMessage
import com.google.protobuf.TypeRegistry
import com.google.type.Interval
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.eventdataprovider.shareshuffle.v2alpha.VidIndexMap
import java.time.Instant
import kotlin.system.measureTimeMillis

@RunWith(JUnit4::class)
class ParallelBatchedPipelineTest {

  private val mockVidIndexMap = mock<VidIndexMap> {
    on { get(any()) } doReturn 0  // Use index 0 instead of 1
    on { size } doReturn 10  // Mock size to ensure the vector is large enough
  }

  private val testTypeRegistry: TypeRegistry = TypeRegistry.newBuilder()
    .add(TestEvent.getDescriptor())
    .build()

  private fun createDynamicMessage(testEvent: TestEvent): DynamicMessage {
    return DynamicMessage.newBuilder(TestEvent.getDescriptor())
      .mergeFrom(testEvent)
      .build()
  }

  @Test
  fun `processEventBatches processes empty flow successfully`() = runBlocking {
    val pipeline = ParallelBatchedPipeline(
      batchSize = 10,
      workers = 2,
      dispatcher = Dispatchers.Default
    )

    val result = pipeline.processEventBatches(
      eventBatchFlow = flowOf(),
      vidIndexMap = mockVidIndexMap,
      filters = listOf(
        FilterConfiguration(
          filterSpec = FilterSpec(
            celExpression = "", // Empty expression matches all events
            collectionInterval = Interval.getDefaultInstance(),
            vidSamplingStart = 0L,
            vidSamplingWidth = 1000000L,
            eventGroupReferenceId = "reference-id-1"
          ),
          requisitionNames = setOf("filter1")
        )
      ),
      typeRegistry = testTypeRegistry
    )

    assertThat(result).hasSize(1)
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
      vid = 12345L,
      message = createDynamicMessage(testEvent)
    )

    val result = pipeline.processEventBatches(
      eventBatchFlow = flowOf(listOf(labeledEvent)),
      vidIndexMap = mockVidIndexMap,
      filters = listOf(
        FilterConfiguration(
          filterSpec = FilterSpec(
            celExpression = "", // Empty expression matches all events
            collectionInterval = Interval.newBuilder()
              .setStartTime(com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(eventTimestamp.minusSeconds(3600).epochSecond)
                .build())
              .setEndTime(com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(eventTimestamp.plusSeconds(3600).epochSecond)
                .build())
              .build(),
            vidSamplingStart = 0L,
            vidSamplingWidth = 1000000L,
            eventGroupReferenceId = "reference-id-1"
          ),
          requisitionNames = setOf("filter1")
        )
      ),
      typeRegistry = testTypeRegistry
    )

    assertThat(result).hasSize(1)
    val frequencyVector = result.values.first()
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
      vid = 12345L,
      message = createDynamicMessage(testEvent)
    )

    val timeInterval = Interval.newBuilder()
      .setStartTime(com.google.protobuf.Timestamp.newBuilder()
        .setSeconds(System.currentTimeMillis() / 1000 - 3600))
      .setEndTime(com.google.protobuf.Timestamp.newBuilder()
        .setSeconds(System.currentTimeMillis() / 1000))
      .build()

    val result = pipeline.processEventBatches(
      eventBatchFlow = flowOf(listOf(labeledEvent)),
      vidIndexMap = mockVidIndexMap,
      filters = listOf(
        FilterConfiguration(
          filterSpec = FilterSpec(
            celExpression = "", // Empty expression matches all events
            collectionInterval = timeInterval,
            vidSamplingStart = 0L,
            vidSamplingWidth = 1000000L,
            eventGroupReferenceId = "reference-id-1"
          ),
          requisitionNames = setOf("timeFilter")
        )
      ),
      typeRegistry = testTypeRegistry
    )

    assertThat(result).hasSize(1)
  }

  @Test
  fun `pipelineType returns correct value`() {
    val pipeline = ParallelBatchedPipeline(
      batchSize = 10,
      workers = 2,
      dispatcher = Dispatchers.Default
    )

    assertThat(pipeline.pipelineType).isEqualTo("Parallel-Batched")
  }

  @Test
  fun `processEventBatches with different batch sizes`() = runBlocking {
    val smallBatchPipeline = ParallelBatchedPipeline(
      batchSize = 2,
      workers = 1,
      dispatcher = Dispatchers.Default
    )

    val events = (1..3).map { i ->
      LabeledEvent(
        timestamp = Instant.now(),
        vid = i.toLong(),
        message = createDynamicMessage(TestEvent.newBuilder()
          .setPerson(Person.newBuilder().setAgeGroup(Person.AgeGroup.YEARS_18_TO_34))
          .build())
      )
    }

    val result = smallBatchPipeline.processEventBatches(
      eventBatchFlow = flowOf(events),
      vidIndexMap = mockVidIndexMap,
      filters = listOf(
        FilterConfiguration(
          filterSpec = FilterSpec(
            celExpression = "", // Empty expression matches all events
            collectionInterval = Interval.getDefaultInstance(),
            vidSamplingStart = 0L,
            vidSamplingWidth = 1000000L,
            eventGroupReferenceId = "reference-id-1"
          ),
          requisitionNames = setOf("testFilter")
        )
      ),
      typeRegistry = testTypeRegistry
    )

    assertThat(result).hasSize(1)
  }

  @Test
  fun `processEventBatches with multiple workers`() = runBlocking {
    val pipeline = ParallelBatchedPipeline(
      batchSize = 1,
      workers = 4,
      dispatcher = Dispatchers.Default
    )

    val events = (1..8).map { i ->
      LabeledEvent(
        timestamp = Instant.now(),
        vid = i.toLong(),
        message = createDynamicMessage(TestEvent.newBuilder()
          .setPerson(Person.newBuilder().setAgeGroup(Person.AgeGroup.YEARS_18_TO_34))
          .build())
      )
    }

    val result = pipeline.processEventBatches(
      eventBatchFlow = flowOf(events),
      vidIndexMap = mockVidIndexMap,
      filters = listOf(
        FilterConfiguration(
          filterSpec = FilterSpec(
            celExpression = "", // Empty expression matches all events
            collectionInterval = Interval.getDefaultInstance(),
            vidSamplingStart = 0L,
            vidSamplingWidth = 1000000L,
            eventGroupReferenceId = "reference-id-1"
          ),
          requisitionNames = setOf("parallelFilter")
        )
      ),
      typeRegistry = testTypeRegistry
    )

    assertThat(result).hasSize(1)
  }

  @Test
  fun `pipelineType returns correct value again`() {
    val pipeline = ParallelBatchedPipeline(
      batchSize = 10,
      workers = 2,
      dispatcher = Dispatchers.Default
    )

    assertThat(pipeline.pipelineType).isEqualTo("Parallel-Batched")
  }
}
