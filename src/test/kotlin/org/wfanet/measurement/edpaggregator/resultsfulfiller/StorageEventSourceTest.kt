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
import com.google.crypto.tink.KmsClient
import com.google.protobuf.Any
import com.google.protobuf.TypeRegistry
import java.time.LocalDate
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.edpaggregator.StorageConfig
import java.time.Instant

@RunWith(JUnit4::class)
class StorageEventSourceTest {

  private val mockEventReader: EventReader = mock()
  private val mockKmsClient: KmsClient = mock()
  private val mockStorageConfig: StorageConfig = StorageConfig()
  private val mockTypeRegistry: TypeRegistry = TypeRegistry.getEmptyTypeRegistry()

  @Test
  fun `generateEventBatches returns batched events from storage`() {
    runBlocking {
    val dateRange = LocalDate.of(2025, 1, 1)..LocalDate.of(2025, 1, 2)
    val eventGroupReferenceIds = listOf("event-group-1", "event-group-2")
    val batchSize = 2

    val testEvent1 = testEvent { person = person { gender = Person.Gender.MALE } }
    val testEvent2 = testEvent { person = person { gender = Person.Gender.FEMALE } }
    
    val labeledEvent1 = LabeledEvent(
      timestamp = Instant.now(),
      vid = 1L,
      message = testEvent1,
      eventGroupReferenceId = "event-group-1"
    )
    val labeledEvent2 = LabeledEvent(
      timestamp = Instant.now(),
      vid = 2L,
      message = testEvent2,
      eventGroupReferenceId = "event-group-1"
    )
    val labeledEvent3 = LabeledEvent(
      timestamp = Instant.now(),
      vid = 3L,
      message = testEvent1,
      eventGroupReferenceId = "event-group-2"
    )

    whenever(mockEventReader.getLabeledEventsBatched(eq(LocalDate.of(2025, 1, 1)), eq("event-group-1"), eq(batchSize)))
      .thenReturn(flowOf(listOf(labeledEvent1, labeledEvent2)))
    
    whenever(mockEventReader.getLabeledEventsBatched(eq(LocalDate.of(2025, 1, 1)), eq("event-group-2"), eq(batchSize)))
      .thenReturn(flowOf(listOf(labeledEvent3)))
    
    whenever(mockEventReader.getLabeledEventsBatched(eq(LocalDate.of(2025, 1, 2)), any(), eq(batchSize)))
      .thenReturn(flowOf())

    val eventSource = StorageEventSource(
      eventReader = mockEventReader,
      dateRange = dateRange,
      eventGroupReferenceIds = eventGroupReferenceIds,
      batchSize = batchSize
    )

    val batches = eventSource.generateEventBatches().toList()

    // Due to parallel processing, batches may be sent in different orders
    // We should verify the total number of events across all batches
    val totalEvents = batches.flatten()
    assertThat(totalEvents).hasSize(3)
    
    val vids = totalEvents.map { it.vid }.sorted()
    assertThat(vids).containsExactly(1L, 2L, 3L)
    }
  }

  @Test
  fun `generateEventBatches handles empty date range`() = runBlocking {
    val dateRange = LocalDate.of(2025, 1, 2)..LocalDate.of(2025, 1, 1)
    val eventGroupReferenceIds = listOf("event-group-1")

    val eventSource = StorageEventSource(
      eventReader = mockEventReader,
      dateRange = dateRange,
      eventGroupReferenceIds = eventGroupReferenceIds
    )

    val batches = eventSource.generateEventBatches().toList()

    assertThat(batches).isEmpty()
  }

  @Test
  fun `generateEventBatches continues processing when ImpressionReadException occurs`() = runBlocking {
    val dateRange = LocalDate.of(2025, 1, 1)..LocalDate.of(2025, 1, 1)
    val eventGroupReferenceIds = listOf("event-group-1", "event-group-2")
    val batchSize = 10

    val testEvent1 = testEvent { person = person { gender = Person.Gender.MALE } }
    val labeledEvent1 = LabeledEvent(
      timestamp = Instant.now(),
      vid = 1L,
      message = testEvent1,
      eventGroupReferenceId = "event-group-2"
    )

    doThrow(RuntimeException("Simulated ImpressionReadException"))
      .whenever(mockEventReader).getLabeledEventsBatched(eq(LocalDate.of(2025, 1, 1)), eq("event-group-1"), eq(batchSize))
    
    whenever(mockEventReader.getLabeledEventsBatched(eq(LocalDate.of(2025, 1, 1)), eq("event-group-2"), eq(batchSize)))
      .thenReturn(flowOf(listOf(labeledEvent1)))

    val eventSource = StorageEventSource(
      eventReader = mockEventReader,
      dateRange = dateRange,
      eventGroupReferenceIds = eventGroupReferenceIds,
      batchSize = batchSize
    )

    val batches = eventSource.generateEventBatches().toList()

    assertThat(batches).hasSize(1)
    assertThat(batches[0]).hasSize(1)
    assertThat(batches[0][0].vid).isEqualTo(1L)
  }

  @Test
  fun `processes multiple dates in parallel`() {
    runBlocking {
    val dateRange = LocalDate.of(2025, 1, 1)..LocalDate.of(2025, 1, 3)
    val eventGroupReferenceIds = listOf("event-group-1")
    val batchSize = 10

    val testEvent1 = testEvent { person = person { gender = Person.Gender.MALE } }
    
    val labeledEvent1 = LabeledEvent(
      timestamp = Instant.now(),
      vid = 1L,
      message = testEvent1,
      eventGroupReferenceId = "event-group-1"
    )
    val labeledEvent2 = LabeledEvent(
      timestamp = Instant.now(),
      vid = 2L,
      message = testEvent1,
      eventGroupReferenceId = "event-group-1"
    )
    val labeledEvent3 = LabeledEvent(
      timestamp = Instant.now(),
      vid = 3L,
      message = testEvent1,
      eventGroupReferenceId = "event-group-1"
    )

    whenever(mockEventReader.getLabeledEventsBatched(eq(LocalDate.of(2025, 1, 1)), eq("event-group-1"), eq(batchSize)))
      .thenReturn(flowOf(listOf(labeledEvent1)))
    
    whenever(mockEventReader.getLabeledEventsBatched(eq(LocalDate.of(2025, 1, 2)), eq("event-group-1"), eq(batchSize)))
      .thenReturn(flowOf(listOf(labeledEvent2)))
    
    whenever(mockEventReader.getLabeledEventsBatched(eq(LocalDate.of(2025, 1, 3)), eq("event-group-1"), eq(batchSize)))
      .thenReturn(flowOf(listOf(labeledEvent3)))

    val eventSource = StorageEventSource(
      eventReader = mockEventReader,
      dateRange = dateRange,
      eventGroupReferenceIds = eventGroupReferenceIds,
      batchSize = batchSize
    )

    val batches = eventSource.generateEventBatches().toList()

    assertThat(batches).hasSize(3)
    val allVids = batches.flatten().map { it.vid }.sorted()
    assertThat(allVids).containsExactly(1L, 2L, 3L)
    }
  }

  @Test
  fun `secondary constructor creates EventReader internally`() = runBlocking {
    val dateRange = LocalDate.of(2025, 1, 1)..LocalDate.of(2025, 1, 1)
    val eventGroupReferenceIds = listOf("event-group-1")

    val eventSource = StorageEventSource(
      kmsClient = mockKmsClient,
      impressionsStorageConfig = mockStorageConfig,
      impressionDekStorageConfig = mockStorageConfig,
      labeledImpressionsDekPrefix = "test-prefix",
      typeRegistry = mockTypeRegistry,
      dateRange = dateRange,
      eventGroupReferenceIds = eventGroupReferenceIds,
      batchSize = 10
    )

    assertThat(eventSource).isNotNull()
  }

  @Test
  fun `generateEventBatches handles larger batch sizes correctly`() {
    runBlocking {
    val dateRange = LocalDate.of(2025, 1, 1)..LocalDate.of(2025, 1, 1)
    val eventGroupReferenceIds = listOf("event-group-1")
    val batchSize = 3

    val testEvent1 = testEvent { person = person { gender = Person.Gender.MALE } }
    
    // Create 5 events
    val events = (1L..5L).map { vid ->
      LabeledEvent(
        timestamp = Instant.now(),
        vid = vid,
        message = testEvent1,
        eventGroupReferenceId = "event-group-1"
      )
    }

    whenever(mockEventReader.getLabeledEventsBatched(eq(LocalDate.of(2025, 1, 1)), eq("event-group-1"), eq(batchSize)))
      .thenReturn(flowOf(events.take(3), events.drop(3)))

    val eventSource = StorageEventSource(
      eventReader = mockEventReader,
      dateRange = dateRange,
      eventGroupReferenceIds = eventGroupReferenceIds,
      batchSize = batchSize
    )

    val batches = eventSource.generateEventBatches().toList()

    // Should have 2 batches: [1,2,3] and [4,5]
    assertThat(batches).hasSize(2)
    assertThat(batches[0]).hasSize(3)
    assertThat(batches[0].map { it.vid }).containsExactly(1L, 2L, 3L)
    assertThat(batches[1]).hasSize(2)
    assertThat(batches[1].map { it.vid }).containsExactly(4L, 5L)
    }
  }
}