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
import org.wfanet.measurement.loadtest.dataprovider.LabeledEvent
import java.time.Instant
import kotlin.system.measureTimeMillis

@RunWith(JUnit4::class)
class ParallelBatchedPipelineTest {

  private val mockVidIndexMap = mock<VidIndexMap> {
    on { get(any()) } doReturn 1
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
          filterId = "filter1",
          celExpression = "person.age_group == 1"
        )
      ),
      typeRegistry = testTypeRegistry
    )

    assertThat(result).hasSize(1)
    assertThat(result).containsKey("filter1")
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

    val labeledEvent = LabeledEvent(
      timestamp = Instant.now(),
      vid = 12345L,
      message = createDynamicMessage(testEvent)
    )

    val result = pipeline.processEventBatches(
      eventBatchFlow = flowOf(listOf(labeledEvent)),
      vidIndexMap = mockVidIndexMap,
      filters = listOf(
        FilterConfiguration(
          filterId = "filter1",
          celExpression = "person.age_group == 1"
        )
      ),
      typeRegistry = testTypeRegistry
    )

    assertThat(result).hasSize(1)
    assertThat(result).containsKey("filter1")
    val stats = result["filter1"]!!
    assertThat(stats.sinkId).isEqualTo("filter1")
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
          filterId = "timeFilter",
          celExpression = "person.age_group == 1",
          timeInterval = timeInterval
        )
      ),
      typeRegistry = testTypeRegistry
    )

    assertThat(result).hasSize(1)
    assertThat(result).containsKey("timeFilter")
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
          filterId = "testFilter",
          celExpression = "person.age_group == 1"
        )
      ),
      typeRegistry = testTypeRegistry
    )

    assertThat(result).hasSize(1)
    assertThat(result).containsKey("testFilter")
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
          filterId = "parallelFilter",
          celExpression = "person.age_group == 1"
        )
      ),
      typeRegistry = testTypeRegistry
    )

    assertThat(result).hasSize(1)
    assertThat(result).containsKey("parallelFilter")
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
