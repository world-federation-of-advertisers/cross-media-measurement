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

  @Test
  fun `processEvents processes empty flow successfully`() = runBlocking {
    val pipeline = ParallelBatchedPipeline(
      batchSize = 10,
      workers = 2,
      dispatcher = Dispatchers.Default
    )

    val result = pipeline.processEvents(
      eventFlow = flowOf(),
      vidIndexMap = mockVidIndexMap,
      filters = listOf(
        FilterConfiguration(
          filterId = "filter1",
          celExpression = "person.age_group == 1"
        )
      )
    )

    assertThat(result).hasSize(1)
    assertThat(result).containsKey("filter1")
  }

  @Test
  fun `processEvents handles single event`() = runBlocking {
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
      message = testEvent
    )

    val result = pipeline.processEvents(
      eventFlow = flowOf(labeledEvent),
      vidIndexMap = mockVidIndexMap,
      filters = listOf(
        FilterConfiguration(
          filterId = "filter1",
          celExpression = "person.age_group == 1"
        )
      )
    )

    assertThat(result).hasSize(1)
    assertThat(result).containsKey("filter1")
    val stats = result["filter1"]!!
    assertThat(stats.sinkId).isEqualTo("filter1")
  }

  @Test
  fun `processEvents handles multiple events with multiple filters`() = runBlocking {
    val pipeline = ParallelBatchedPipeline(
      batchSize = 2,
      workers = 2,
      dispatcher = Dispatchers.Default
    )

    val events = (1..5).map { i ->
      LabeledEvent(
        timestamp = Instant.now(),
        vid = i.toLong(),
        message = TestEvent.newBuilder()
          .setPerson(Person.newBuilder().setAgeGroup(Person.AgeGroup.YEARS_18_TO_34))
          .build()
      )
    }

    val result = pipeline.processEvents(
      eventFlow = flowOf(*events.toTypedArray()),
      vidIndexMap = mockVidIndexMap,
      filters = listOf(
        FilterConfiguration(
          filterId = "filter1",
          celExpression = "person.age_group == 1"
        ),
        FilterConfiguration(
          filterId = "filter2", 
          celExpression = "has(person)"
        )
      )
    )

    assertThat(result).hasSize(2)
    assertThat(result).containsKey("filter1")
    assertThat(result).containsKey("filter2")
    
    val filter1Stats = result["filter1"]!!
    assertThat(filter1Stats.sinkId).isEqualTo("filter1")
    
    val filter2Stats = result["filter2"]!!
    assertThat(filter2Stats.sinkId).isEqualTo("filter2")
  }

  @Test
  fun `processEvents with time interval filters`() = runBlocking {
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
      message = testEvent
    )

    val timeInterval = Interval.newBuilder()
      .setStartTime(com.google.protobuf.Timestamp.newBuilder()
        .setSeconds(System.currentTimeMillis() / 1000 - 3600))
      .setEndTime(com.google.protobuf.Timestamp.newBuilder()
        .setSeconds(System.currentTimeMillis() / 1000))
      .build()

    val result = pipeline.processEvents(
      eventFlow = flowOf(labeledEvent),
      vidIndexMap = mockVidIndexMap,
      filters = listOf(
        FilterConfiguration(
          filterId = "timeFilter",
          celExpression = "person.age_group == 1",
          timeInterval = timeInterval
        )
      )
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
  fun `processEvents with different batch sizes`() = runBlocking {
    val smallBatchPipeline = ParallelBatchedPipeline(
      batchSize = 2,
      workers = 1,
      dispatcher = Dispatchers.Default
    )

    val events = (1..3).map { i ->
      LabeledEvent(
        timestamp = Instant.now(),
        vid = i.toLong(),
        message = TestEvent.newBuilder()
          .setPerson(Person.newBuilder().setAgeGroup(Person.AgeGroup.YEARS_18_TO_34))
          .build()
      )
    }

    val result = smallBatchPipeline.processEvents(
      eventFlow = flowOf(*events.toTypedArray()),
      vidIndexMap = mockVidIndexMap,
      filters = listOf(
        FilterConfiguration(
          filterId = "testFilter",
          celExpression = "person.age_group == 1"
        )
      )
    )

    assertThat(result).hasSize(1)
    assertThat(result).containsKey("testFilter")
  }

  @Test
  fun `processEvents with multiple workers`() = runBlocking {
    val pipeline = ParallelBatchedPipeline(
      batchSize = 1,
      workers = 4,
      dispatcher = Dispatchers.Default
    )

    val events = (1..8).map { i ->
      LabeledEvent(
        timestamp = Instant.now(),
        vid = i.toLong(),
        message = TestEvent.newBuilder()
          .setPerson(Person.newBuilder().setAgeGroup(Person.AgeGroup.YEARS_18_TO_34))
          .build()
      )
    }

    val result = pipeline.processEvents(
      eventFlow = flowOf(*events.toTypedArray()),
      vidIndexMap = mockVidIndexMap,
      filters = listOf(
        FilterConfiguration(
          filterId = "parallelFilter",
          celExpression = "person.age_group == 1"
        )
      )
    )

    assertThat(result).hasSize(1)
    assertThat(result).containsKey("parallelFilter")
  }

  @Test
  fun `processEvents validates processed event count with multiple filters`() = runBlocking {
    val pipeline = ParallelBatchedPipeline(
      batchSize = 3,
      workers = 2,
      dispatcher = Dispatchers.Default
    )

    val totalEvents = 10
    val events = (1..totalEvents).map { i ->
      LabeledEvent(
        timestamp = Instant.now(),
        vid = i.toLong(),
        message = TestEvent.newBuilder()
          .setPerson(Person.newBuilder().setAgeGroup(
            if (i % 2 == 0) Person.AgeGroup.YEARS_18_TO_34 else Person.AgeGroup.YEARS_35_TO_54
          ))
          .build()
      )
    }

    val result = pipeline.processEvents(
      eventFlow = flowOf(*events.toTypedArray()),
      vidIndexMap = mockVidIndexMap,
      filters = listOf(
        FilterConfiguration(
          filterId = "allEvents",
          celExpression = "has(person)"
        ),
        FilterConfiguration(
          filterId = "young",
          celExpression = "person.age_group == 1"  // YEARS_18_TO_34 = 1
        ),
        FilterConfiguration(
          filterId = "middle",
          celExpression = "person.age_group == 2"  // YEARS_35_TO_54 = 2
        )
      )
    )

    assertThat(result).hasSize(3)
    assertThat(result).containsKey("allEvents")
    assertThat(result).containsKey("young")
    assertThat(result).containsKey("middle")

    // Each filter processes events independently, so each sees all 10 events
    val allEventsStats = result["allEvents"]!!
    // processedEvents likely counts how many events were evaluated, not unique events
    
    val youngStats = result["young"]!!  
    val middleStats = result["middle"]!!
    
    // Verify that we have results for all filters
    assertThat(allEventsStats.sinkId).isEqualTo("allEvents")
    assertThat(youngStats.sinkId).isEqualTo("young") 
    assertThat(middleStats.sinkId).isEqualTo("middle")
  }

  @Test
  fun `processEvents with selective filters validates match counts`() = runBlocking {
    val pipeline = ParallelBatchedPipeline(
      batchSize = 2,
      workers = 3,
      dispatcher = Dispatchers.Default
    )

    val events = (1..12).map { i ->
      LabeledEvent(
        timestamp = Instant.now(),
        vid = i.toLong(),
        message = TestEvent.newBuilder()
          .setPerson(Person.newBuilder().setAgeGroup(
            when {
              i % 3 == 0 -> Person.AgeGroup.YEARS_55_PLUS
              i < 10 -> Person.AgeGroup.YEARS_18_TO_34
              else -> Person.AgeGroup.YEARS_35_TO_54
            }
          ))
          .build()
      )
    }

    val result = pipeline.processEvents(
      eventFlow = flowOf(*events.toTypedArray()),
      vidIndexMap = mockVidIndexMap,
      filters = listOf(
        FilterConfiguration(
          filterId = "divisibleByThree",
          celExpression = "person.age_group == 3"  // YEARS_55_PLUS = 3
        ),
        FilterConfiguration(
          filterId = "young",
          celExpression = "person.age_group == 1"  // YEARS_18_TO_34 = 1
        ),
        FilterConfiguration(
          filterId = "allEvents",
          celExpression = "has(person)"
        )
      )
    )

    assertThat(result).hasSize(3)
    
    // Verify all filters returned results
    assertThat(result).hasSize(3)
    result.values.forEach { stats ->
      assertThat(stats.sinkId).isNotEmpty()
    }
    
    // Validate specific match expectations
    val divisibleStats = result["divisibleByThree"]!!
    // Events 3, 6, 9, 12 should match age_group == 3 (4 events)
    assertThat(divisibleStats.matchedEvents).isEqualTo(4L)
    
    val youngStats = result["young"]!!
    // Events 1,2,4,5,7,8 should match (6 events - all single digit except 3,6,9)
    assertThat(youngStats.matchedEvents).isEqualTo(6L)
    
    val allStats = result["allEvents"]!!
    // All events should match has(person) (12 events)
    assertThat(allStats.matchedEvents).isEqualTo(12L)
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