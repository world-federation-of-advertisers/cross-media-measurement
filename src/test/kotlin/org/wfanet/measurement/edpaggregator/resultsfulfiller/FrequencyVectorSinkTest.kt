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
import com.google.type.Interval
import java.time.Instant
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
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.VidIndexMap

@RunWith(JUnit4::class)
class FrequencyVectorSinkTest {

  companion object {
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
    whenever(mockVidIndexMap[100L]).thenReturn(0)
    whenever(mockVidIndexMap[200L]).thenReturn(1)
    whenever(mockVidIndexMap[300L]).thenReturn(2)

    val sink = FrequencyVectorSink(mockFrequencyVector, mockVidIndexMap)

    val matchedEvents = listOf(
      createTestEvent(vid = 100L),
      createTestEvent(vid = 200L),
      createTestEvent(vid = 300L)
    )

    sink.processMatchedEvents(matchedEvents)

    verify(mockFrequencyVector, times(3)).incrementByIndex(any())
    verify(mockFrequencyVector).incrementByIndex(0)
    verify(mockFrequencyVector).incrementByIndex(1)
    verify(mockFrequencyVector).incrementByIndex(2)
  }

  @Test
  fun `processMatchedEvents handles empty list`() = runBlocking {
    val mockFrequencyVector = mock<FrequencyVector>()
    val mockVidIndexMap = mock<VidIndexMap>()

    val sink = FrequencyVectorSink(mockFrequencyVector, mockVidIndexMap)

    sink.processMatchedEvents(emptyList())

    verify(mockFrequencyVector, never()).incrementByIndex(any())
  }

  @Test
  fun `getFrequencyVector returns frequency vector`() {
    val mockFrequencyVector = mock<FrequencyVector>()
    val mockVidIndexMap = mock<VidIndexMap>()

    val sink = FrequencyVectorSink(mockFrequencyVector, mockVidIndexMap)

    val result = sink.getFrequencyVector()

    assertThat(result).isSameInstanceAs(mockFrequencyVector)
  }

  @Test
  fun `processMatchedEvents processes multiple events`() = runBlocking {
    val mockFrequencyVector = mock<FrequencyVector>()
    val mockVidIndexMap = mock<VidIndexMap>()
    (1..10).forEach { whenever(mockVidIndexMap[it.toLong()]).thenReturn(it - 1) }

    val sink = FrequencyVectorSink(mockFrequencyVector, mockVidIndexMap)

    val matchedEvents = (1..10).map { createTestEvent(vid = it.toLong()) }
    sink.processMatchedEvents(matchedEvents)

    // Verify all 10 events were processed
    verify(mockFrequencyVector, times(10)).incrementByIndex(any())
    (0..9).forEach { index ->
      verify(mockFrequencyVector).incrementByIndex(index)
    }
  }

  @Test
  fun `multiple calls accumulate correctly`() = runBlocking {
    val mockFrequencyVector = mock<FrequencyVector>()
    val mockVidIndexMap = mock<VidIndexMap>()
    (1..5).forEach { whenever(mockVidIndexMap[it.toLong()]).thenReturn(it - 1) }

    val sink = FrequencyVectorSink(mockFrequencyVector, mockVidIndexMap)

    sink.processMatchedEvents(
      listOf(createTestEvent(1L), createTestEvent(2L))
    )

    sink.processMatchedEvents(
      listOf(createTestEvent(3L), createTestEvent(4L), createTestEvent(5L))
    )

    // Verify that all 5 events were processed across both calls
    verify(mockFrequencyVector, times(5)).incrementByIndex(any())
    (0..4).forEach { index ->
      verify(mockFrequencyVector).incrementByIndex(index)
    }
  }
}
