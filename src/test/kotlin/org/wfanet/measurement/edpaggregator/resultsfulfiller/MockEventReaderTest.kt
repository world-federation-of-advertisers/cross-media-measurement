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
import java.time.LocalDate
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import kotlinx.coroutines.Dispatchers

@RunWith(JUnit4::class)
class MockEventReaderTest {

  @Test
  fun `MockEventReader empty returns no events`(): Unit = runBlocking {
    val mockReader = MockEventReader.empty()
    val batches = mockReader.readEvents().toList()
    assertThat(batches).isEmpty()
  }

  @Test
  fun `MockEventReader withEventCount returns correct number of events`(): Unit = runBlocking {
    val mockReader = MockEventReader.withEventCount(3)
    val batches = mockReader.readEvents().toList()
    
    assertThat(batches).hasSize(1)
    assertThat(batches[0]).hasSize(3)
    assertThat(batches[0].map { it.eventGroupReferenceId }).containsExactly(
      "test-group-event-1",
      "test-group-event-2", 
      "test-group-event-3"
    )
  }

  @Test
  fun `MockEventReader withBatches returns multiple batches`(): Unit = runBlocking {
    val mockReader = MockEventReader.withBatches(2, 3, 1)
    val batches = mockReader.readEvents().toList()
    
    assertThat(batches).hasSize(3)
    assertThat(batches[0]).hasSize(2) // First batch: 2 events
    assertThat(batches[1]).hasSize(3) // Second batch: 3 events
    assertThat(batches[2]).hasSize(1) // Third batch: 1 event
  }

  @Test
  fun `MockEventReader withException throws exception`(): Unit = runBlocking {
    val mockReader = MockEventReader.withException(RuntimeException("Test error"))
    
    try {
      mockReader.readEvents().toList()
      assert(false) { "Expected exception to be thrown" }
    } catch (e: RuntimeException) {
      assertThat(e.message).isEqualTo("Test error")
    }
  }

  @Test
  fun `MockEventReaderFactory withDefault returns same reader for all paths`() {
    val mockReader = MockEventReader.withEventCount(5)
    val factory = MockEventReaderFactory.withDefault(mockReader)
    
    val reader1 = factory.createEventReader("path1/blob", "path1/metadata")
    val reader2 = factory.createEventReader("path2/blob", "path2/metadata")
    
    assertThat(reader1).isSameInstanceAs(mockReader)
    assertThat(reader2).isSameInstanceAs(mockReader)
  }

  @Test
  fun `MockEventReaderFactory withMappings returns path-specific readers`() {
    val reader1 = MockEventReader.withEventCount(1)
    val reader2 = MockEventReader.withEventCount(2)
    
    val mappings = mapOf(
      Pair("blob1", "meta1") to reader1,
      Pair("blob2", "meta2") to reader2
    )
    val factory = MockEventReaderFactory.withMappings(mappings)
    
    assertThat(factory.createEventReader("blob1", "meta1")).isSameInstanceAs(reader1)
    assertThat(factory.createEventReader("blob2", "meta2")).isSameInstanceAs(reader2)
    assertThat(factory.createEventReader("unknown", "unknown")).isInstanceOf(MockEventReader::class.java)
  }

  @Test
  fun `StorageEventSource works with MockEventReaderFactory`(): Unit = runBlocking {
    val mockPathResolver = object : EventPathResolver {
      override suspend fun resolvePaths(date: LocalDate, eventGroupReferenceId: String): EventPathResolver.EventPaths {
        return EventPathResolver.EventPaths("blob-$date", "meta-$date", eventGroupReferenceId)
      }
    }

    val mockReader = MockEventReader.withEventCount(2)
    val mockFactory = MockEventReaderFactory.withDefault(mockReader)

    val eventGroupDetails = createEventGroupDetails("group-1", LocalDate.of(2025, 1, 1), LocalDate.of(2025, 1, 1))
    
    val eventSource = StorageEventSource(
      pathResolver = mockPathResolver,
      eventReaderFactory = mockFactory,
      eventGroupDetailsList = listOf(eventGroupDetails)
    )

    val batches = eventSource.generateEventBatches(Dispatchers.Unconfined).toList()

    assertThat(batches).hasSize(1)
    assertThat(batches[0].events).hasSize(2)
    assertThat(batches[0].batchId).isEqualTo(1L)
  }

  private fun createEventGroupDetails(eventGroupReferenceId: String, startDate: LocalDate, endDate: LocalDate): GroupedRequisitions.EventGroupDetails {
    val startInstant = startDate.atStartOfDay().toInstant(java.time.ZoneOffset.UTC)
    val endInstant = endDate.atStartOfDay().toInstant(java.time.ZoneOffset.UTC)
    
    return GroupedRequisitions.EventGroupDetails.newBuilder()
      .setEventGroupReferenceId(eventGroupReferenceId)
      .addCollectionIntervals(
        interval {
          startTime = timestamp {
            seconds = startInstant.epochSecond
            nanos = startInstant.nano
          }
          endTime = timestamp {
            seconds = endInstant.epochSecond
            nanos = endInstant.nano
          }
        }
      )
      .build()
  }
}