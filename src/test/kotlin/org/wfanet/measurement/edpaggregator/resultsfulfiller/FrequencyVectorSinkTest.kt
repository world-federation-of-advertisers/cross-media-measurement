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
import com.google.protobuf.timestamp
import com.google.type.interval
import java.time.Instant
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.*
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.VidIndexMap

@RunWith(JUnit4::class)
class FrequencyVectorSinkTest {

  companion object {
    private val testInstant = Instant.now()

    private fun createTestEvent(vid: Long): LabeledEvent<TestEvent> {
      return LabeledEvent(
        timestamp = testInstant,
        vid = vid,
        message = TestEvent.getDefaultInstance(),
      )
    }

    private fun createEventBatch(events: List<LabeledEvent<TestEvent>>): EventBatch<TestEvent> {
      return EventBatch(
        events = events,
        minTime = testInstant,
        maxTime = testInstant,
        eventGroupReferenceId = "test-group",
      )
    }

    private fun createFilterSpec(): FilterSpec {
      val interval = interval {
        startTime = timestamp { seconds = testInstant.epochSecond }
        endTime = timestamp { seconds = testInstant.epochSecond + 3600 }
      }
      return FilterSpec(
        celExpression = "true",
        collectionInterval = interval,
        eventGroupReferenceIds = listOf("test-event-group-1"),
      )
    }
  }

  @Test
  fun `processBatch updates frequency vector correctly`() = runBlocking {
    val mockFrequencyVector = mock<StripedByteFrequencyVector>()
    val mockVidIndexMap = mock<VidIndexMap>()
    val mockFilterProcessor = mock<FilterProcessor<TestEvent>>()
    val filterSpec = createFilterSpec()

    whenever(mockFilterProcessor.filterSpec).thenReturn(filterSpec)
    whenever(mockVidIndexMap[100L]).thenReturn(0)
    whenever(mockVidIndexMap[200L]).thenReturn(1)
    whenever(mockVidIndexMap[300L]).thenReturn(2)

    val sink = FrequencyVectorSink(mockFilterProcessor, mockFrequencyVector, mockVidIndexMap)

    val events =
      listOf(createTestEvent(vid = 100L), createTestEvent(vid = 200L), createTestEvent(vid = 300L))
    val batch = createEventBatch(events)

    whenever(mockFilterProcessor.processBatch(batch)).thenReturn(batch)

    sink.processBatch(batch)

    verify(mockFrequencyVector, times(3)).increment(any())
    verify(mockFrequencyVector).increment(0)
    verify(mockFrequencyVector).increment(1)
    verify(mockFrequencyVector).increment(2)
  }

  @Test
  fun `processBatch handles empty list`() = runBlocking {
    val mockFrequencyVector = mock<StripedByteFrequencyVector>()
    val mockVidIndexMap = mock<VidIndexMap>()
    val mockFilterProcessor = mock<FilterProcessor<TestEvent>>()
    val filterSpec = createFilterSpec()

    whenever(mockFilterProcessor.filterSpec).thenReturn(filterSpec)

    val sink = FrequencyVectorSink(mockFilterProcessor, mockFrequencyVector, mockVidIndexMap)

    val emptyBatch = createEventBatch(emptyList())
    whenever(mockFilterProcessor.processBatch(emptyBatch)).thenReturn(emptyBatch)

    sink.processBatch(emptyBatch)

    verify(mockFrequencyVector, never()).increment(any())
  }

  @Test
  fun `getFrequencyVector returns frequency vector`() {
    val mockFrequencyVector = mock<StripedByteFrequencyVector>()
    val mockVidIndexMap = mock<VidIndexMap>()
    val mockFilterProcessor = mock<FilterProcessor<TestEvent>>()
    val filterSpec = createFilterSpec()

    whenever(mockFilterProcessor.filterSpec).thenReturn(filterSpec)

    val sink = FrequencyVectorSink(mockFilterProcessor, mockFrequencyVector, mockVidIndexMap)

    val result = sink.getFrequencyVector()

    assertThat(result).isSameInstanceAs(mockFrequencyVector)
  }

  @Test
  fun `getFilterSpec returns filter spec`() {
    val mockFrequencyVector = mock<StripedByteFrequencyVector>()
    val mockVidIndexMap = mock<VidIndexMap>()
    val mockFilterProcessor = mock<FilterProcessor<TestEvent>>()
    val filterSpec = createFilterSpec()

    whenever(mockFilterProcessor.filterSpec).thenReturn(filterSpec)

    val sink = FrequencyVectorSink(mockFilterProcessor, mockFrequencyVector, mockVidIndexMap)

    val result = sink.getFilterSpec()

    assertThat(result).isSameInstanceAs(filterSpec)
  }

  @Test
  fun `processBatch processes multiple events`() = runBlocking {
    val mockFrequencyVector = mock<StripedByteFrequencyVector>()
    val mockVidIndexMap = mock<VidIndexMap>()
    val mockFilterProcessor = mock<FilterProcessor<TestEvent>>()
    val filterSpec = createFilterSpec()

    whenever(mockFilterProcessor.filterSpec).thenReturn(filterSpec)
    (1..10).forEach { whenever(mockVidIndexMap[it.toLong()]).thenReturn(it - 1) }

    val sink = FrequencyVectorSink(mockFilterProcessor, mockFrequencyVector, mockVidIndexMap)

    val events = (1..10).map { createTestEvent(vid = it.toLong()) }
    val batch = createEventBatch(events)
    whenever(mockFilterProcessor.processBatch(batch)).thenReturn(batch)

    sink.processBatch(batch)

    // Verify all 10 events were processed
    verify(mockFrequencyVector, times(10)).increment(any())
    (0..9).forEach { index -> verify(mockFrequencyVector).increment(index) }
  }

  @Test
  fun `multiple calls accumulate correctly`() = runBlocking {
    val mockFrequencyVector = mock<StripedByteFrequencyVector>()
    val mockVidIndexMap = mock<VidIndexMap>()
    val mockFilterProcessor = mock<FilterProcessor<TestEvent>>()
    val filterSpec = createFilterSpec()

    whenever(mockFilterProcessor.filterSpec).thenReturn(filterSpec)
    (1..5).forEach { whenever(mockVidIndexMap[it.toLong()]).thenReturn(it - 1) }

    val sink = FrequencyVectorSink(mockFilterProcessor, mockFrequencyVector, mockVidIndexMap)

    val batch1 = createEventBatch(listOf(createTestEvent(1L), createTestEvent(2L)))
    whenever(mockFilterProcessor.processBatch(batch1)).thenReturn(batch1)
    sink.processBatch(batch1)

    val batch2 =
      createEventBatch(listOf(createTestEvent(3L), createTestEvent(4L), createTestEvent(5L)))
    whenever(mockFilterProcessor.processBatch(batch2)).thenReturn(batch2)
    sink.processBatch(batch2)

    // Verify that all 5 events were processed across both calls
    verify(mockFrequencyVector, times(5)).increment(any())
    (0..4).forEach { index -> verify(mockFrequencyVector).increment(index) }
  }

  @Test
  fun `getTotalUncappedImpressions delegates to frequency vector`() {
    val mockFrequencyVector = mock<StripedByteFrequencyVector>()
    val mockVidIndexMap = mock<VidIndexMap>()
    val mockFilterProcessor = mock<FilterProcessor<TestEvent>>()
    val filterSpec = createFilterSpec()

    whenever(mockFilterProcessor.filterSpec).thenReturn(filterSpec)
    whenever(mockFrequencyVector.getTotalUncappedImpressions()).thenReturn(42L)

    val sink = FrequencyVectorSink(mockFilterProcessor, mockFrequencyVector, mockVidIndexMap)

    val result = sink.getTotalUncappedImpressions()

    assertThat(result).isEqualTo(42L)
    verify(mockFrequencyVector).getTotalUncappedImpressions()
  }
}
