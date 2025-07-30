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
import com.google.protobuf.Message
import com.google.type.Interval
import java.time.Instant
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.wfanet.measurement.eventdataprovider.shareshuffle.v2alpha.VidIndexMap

@RunWith(JUnit4::class)
class FrequencyVectorSinkTest {

  companion object {
    private val TEST_FILTER_SPEC = FilterSpec(
      celExpression = "event.video_ad.length_seconds > 5",
      collectionInterval = Interval.getDefaultInstance(),
      vidSamplingStart = 0.0f,
      vidSamplingWidth = 1.0f,
      eventGroupReferenceId = "test-event-group-1"
    )

    private fun createTestEvent(vid: Long): LabeledEvent<DynamicMessage> {
      val mockMessage = mock<DynamicMessage>()
      return LabeledEvent(
        timestamp = Instant.now(),
        vid = vid,
        message = mockMessage,
        eventGroupReferenceId = "test-event-group-1"
      )
    }
  }

  @Test
  fun `processMatchedEvents updates frequency vector correctly`() = runBlocking {
    val mockFrequencyVector = mock<FrequencyVector>()
    val mockVidIndexMap = mock<VidIndexMap>()
    whenever(mockFrequencyVector.getReach()).thenReturn(3L)
    whenever(mockFrequencyVector.getTotalCount()).thenReturn(3L)
    whenever(mockFrequencyVector.getAverageFrequency()).thenReturn(1.0)
    whenever(mockVidIndexMap[100L]).thenReturn(0)
    whenever(mockVidIndexMap[200L]).thenReturn(1)
    whenever(mockVidIndexMap[300L]).thenReturn(2)

    val sink = FrequencyVectorSink(TEST_FILTER_SPEC, mockFrequencyVector, mockVidIndexMap)

    val matchedEvents = listOf(
      createTestEvent(vid = 100L),
      createTestEvent(vid = 200L),
      createTestEvent(vid = 300L)
    )

    sink.processMatchedEvents(matchedEvents, totalProcessed = 10)

    verify(mockFrequencyVector, times(3)).incrementByIndex(any())
    verify(mockFrequencyVector).incrementByIndex(0)
    verify(mockFrequencyVector).incrementByIndex(1)
    verify(mockFrequencyVector).incrementByIndex(2)

    val stats = sink.getStatistics()
    assertThat(stats.processedEvents).isEqualTo(10L)
    assertThat(stats.matchedEvents).isEqualTo(3L)
    assertThat(stats.errorCount).isEqualTo(0L)
  }

  @Test
  fun `processMatchedEvents handles empty list`() = runBlocking {
    val mockFrequencyVector = mock<FrequencyVector>()
    val mockVidIndexMap = mock<VidIndexMap>()
    whenever(mockFrequencyVector.getReach()).thenReturn(0L)
    whenever(mockFrequencyVector.getTotalCount()).thenReturn(0L)
    whenever(mockFrequencyVector.getAverageFrequency()).thenReturn(0.0)

    val sink = FrequencyVectorSink(TEST_FILTER_SPEC, mockFrequencyVector, mockVidIndexMap)

    sink.processMatchedEvents(emptyList(), totalProcessed = 5)

    verify(mockFrequencyVector, never()).incrementByIndex(any())

    val stats = sink.getStatistics()
    assertThat(stats.processedEvents).isEqualTo(5L)
    assertThat(stats.matchedEvents).isEqualTo(0L)
    assertThat(stats.errorCount).isEqualTo(0L)
  }

  @Test
  fun `processMatchedEvents counts errors correctly`() = runBlocking {
    val mockFrequencyVector = mock<FrequencyVector>()
    val mockVidIndexMap = mock<VidIndexMap>()
    whenever(mockFrequencyVector.getReach()).thenReturn(2L)
    whenever(mockFrequencyVector.getTotalCount()).thenReturn(2L)
    whenever(mockFrequencyVector.getAverageFrequency()).thenReturn(1.0)
    whenever(mockVidIndexMap[100L]).thenReturn(0)
    whenever(mockVidIndexMap[200L]).thenThrow(RuntimeException("VID not found"))
    whenever(mockVidIndexMap[300L]).thenReturn(2)

    val sink = FrequencyVectorSink(TEST_FILTER_SPEC, mockFrequencyVector, mockVidIndexMap)

    val matchedEvents = listOf(
      createTestEvent(vid = 100L),
      createTestEvent(vid = 200L), // This will fail
      createTestEvent(vid = 300L)
    )

    sink.processMatchedEvents(matchedEvents, totalProcessed = 10)

    verify(mockFrequencyVector).incrementByIndex(0)
    verify(mockVidIndexMap)[200L]
    verify(mockFrequencyVector).incrementByIndex(2)

    val stats = sink.getStatistics()
    assertThat(stats.errorCount).isEqualTo(1L)
    assertThat(stats.matchedEvents).isEqualTo(3L)
  }

  @Test
  fun `getFrequencyVector returns frequency vector`() {
    val mockFrequencyVector = mock<FrequencyVector>()
    val mockVidIndexMap = mock<VidIndexMap>()

    val sink = FrequencyVectorSink(TEST_FILTER_SPEC, mockFrequencyVector, mockVidIndexMap)

    val result = sink.getFrequencyVector()

    assertThat(result).isSameInstanceAs(mockFrequencyVector)
  }

  @Test
  fun `getStatistics returns correct values`() = runBlocking {
    val mockFrequencyVector = mock<FrequencyVector>()
    val mockVidIndexMap = mock<VidIndexMap>()
    whenever(mockFrequencyVector.getReach()).thenReturn(5L)
    whenever(mockFrequencyVector.getTotalCount()).thenReturn(15L)
    whenever(mockFrequencyVector.getAverageFrequency()).thenReturn(3.0)
    (1..10).forEach { whenever(mockVidIndexMap[it.toLong()]).thenReturn(it - 1) }

    val sink = FrequencyVectorSink(TEST_FILTER_SPEC, mockFrequencyVector, mockVidIndexMap)

    val matchedEvents = (1..10).map { createTestEvent(vid = it.toLong()) }
    sink.processMatchedEvents(matchedEvents, totalProcessed = 20)

    val stats = sink.getStatistics()

    assertThat(stats.sinkId).isEqualTo("test-event-group-1")
    assertThat(stats.description).contains("event.video_ad.length_seconds > 5")
    assertThat(stats.processedEvents).isEqualTo(20L)
    assertThat(stats.matchedEvents).isEqualTo(10L)
    assertThat(stats.reach).isEqualTo(5L)
    assertThat(stats.totalFrequency).isEqualTo(15L)
    assertThat(stats.averageFrequency).isEqualTo(3.0)
    assertThat(stats.matchRate).isWithin(0.01).of(50.0)
    assertThat(stats.errorRate).isEqualTo(0.0)
  }

  @Test
  fun `concurrent access is thread safe`() = runBlocking {
    val mockFrequencyVector = mock<FrequencyVector>()
    val mockVidIndexMap = mock<VidIndexMap>()
    whenever(mockFrequencyVector.getReach()).thenReturn(100L)
    whenever(mockFrequencyVector.getTotalCount()).thenReturn(300L)
    whenever(mockFrequencyVector.getAverageFrequency()).thenReturn(3.0)
    (1..1000).forEach { whenever(mockVidIndexMap[it.toLong()]).thenReturn(it - 1) }

    val sink = FrequencyVectorSink(TEST_FILTER_SPEC, mockFrequencyVector, mockVidIndexMap)

    val jobs = (1..10).map { threadId ->
      async {
        val events = (1..10).map { eventId ->
          createTestEvent(vid = (threadId * 100 + eventId).toLong())
        }
        sink.processMatchedEvents(events, totalProcessed = 20)
      }
    }

    jobs.awaitAll()

    val stats = sink.getStatistics()
    assertThat(stats.processedEvents).isEqualTo(200L) // 10 threads * 20 processed
    assertThat(stats.matchedEvents).isEqualTo(100L) // 10 threads * 10 matched
  }

  @Test
  fun `multiple calls accumulate statistics correctly`() = runBlocking {
    val mockFrequencyVector = mock<FrequencyVector>()
    val mockVidIndexMap = mock<VidIndexMap>()
    whenever(mockFrequencyVector.getReach()).thenReturn(15L)
    whenever(mockFrequencyVector.getTotalCount()).thenReturn(20L)
    whenever(mockFrequencyVector.getAverageFrequency()).thenReturn(20.0 / 15.0)
    (1..5).forEach { whenever(mockVidIndexMap[it.toLong()]).thenReturn(it - 1) }

    val sink = FrequencyVectorSink(TEST_FILTER_SPEC, mockFrequencyVector, mockVidIndexMap)

    sink.processMatchedEvents(
      listOf(createTestEvent(1L), createTestEvent(2L)),
      totalProcessed = 5
    )

    sink.processMatchedEvents(
      listOf(createTestEvent(3L), createTestEvent(4L), createTestEvent(5L)),
      totalProcessed = 10
    )

    val stats = sink.getStatistics()
    assertThat(stats.processedEvents).isEqualTo(15L)
    assertThat(stats.matchedEvents).isEqualTo(5L)
  }

  @Test
  fun `handles duplicate VIDs correctly`() = runBlocking {
    val mockFrequencyVector = mock<FrequencyVector>()
    val mockVidIndexMap = mock<VidIndexMap>()
    whenever(mockFrequencyVector.getReach()).thenReturn(2L)
    whenever(mockFrequencyVector.getTotalCount()).thenReturn(5L)
    whenever(mockFrequencyVector.getAverageFrequency()).thenReturn(2.5)
    whenever(mockVidIndexMap[100L]).thenReturn(0)
    whenever(mockVidIndexMap[200L]).thenReturn(1)

    val sink = FrequencyVectorSink(TEST_FILTER_SPEC, mockFrequencyVector, mockVidIndexMap)

    val matchedEvents = listOf(
      createTestEvent(vid = 100L),
      createTestEvent(vid = 100L),
      createTestEvent(vid = 100L),
      createTestEvent(vid = 200L),
      createTestEvent(vid = 200L)
    )

    sink.processMatchedEvents(matchedEvents, totalProcessed = 10)

    verify(mockFrequencyVector, times(3)).incrementByIndex(0)
    verify(mockFrequencyVector, times(2)).incrementByIndex(1)

    val stats = sink.getStatistics()
    assertThat(stats.matchedEvents).isEqualTo(5L)
  }

  @Test
  fun `getStatistics with zero reach handles division by zero`() = runBlocking {
    val mockFrequencyVector = mock<FrequencyVector>()
    val mockVidIndexMap = mock<VidIndexMap>()
    whenever(mockFrequencyVector.getReach()).thenReturn(0L)
    whenever(mockFrequencyVector.getTotalCount()).thenReturn(0L)
    whenever(mockFrequencyVector.getAverageFrequency()).thenReturn(0.0)

    val sink = FrequencyVectorSink(TEST_FILTER_SPEC, mockFrequencyVector, mockVidIndexMap)

    val stats = sink.getStatistics()

    assertThat(stats.reach).isEqualTo(0L)
    assertThat(stats.totalFrequency).isEqualTo(0L)
    assertThat(stats.averageFrequency).isEqualTo(0.0)
  }
}
