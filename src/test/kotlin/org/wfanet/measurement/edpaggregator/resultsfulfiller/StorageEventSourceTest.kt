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
import com.google.protobuf.TypeRegistry
import com.google.protobuf.timestamp
import com.google.type.interval
import java.time.LocalDate
import kotlin.test.assertFailsWith
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions

@RunWith(JUnit4::class)
class StorageEventSourceTest {

  private val mockKmsClient: KmsClient? = null
  private val mockStorageConfig: StorageConfig = StorageConfig()
  private val mockTypeRegistry: TypeRegistry = TypeRegistry.getEmptyTypeRegistry()

  private fun createMockPathResolver(): EventPathResolver {
    return object : EventPathResolver {
      override suspend fun resolvePaths(date: LocalDate, eventGroupReferenceId: String): EventPathResolver.EventPaths {
        return EventPathResolver.EventPaths("blob-$date-$eventGroupReferenceId", "meta-$date-$eventGroupReferenceId", eventGroupReferenceId)
      }
    }
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

  private fun createEventGroupDetailsWithMultipleIntervals(
    eventGroupReferenceId: String,
    intervals: List<Pair<LocalDate, LocalDate>>
  ): GroupedRequisitions.EventGroupDetails {
    val builder = GroupedRequisitions.EventGroupDetails.newBuilder()
      .setEventGroupReferenceId(eventGroupReferenceId)

    intervals.forEach { (startDate, endDate) ->
      val startInstant = startDate.atStartOfDay().toInstant(java.time.ZoneOffset.UTC)
      val endInstant = endDate.atStartOfDay().toInstant(java.time.ZoneOffset.UTC)

      builder.addCollectionIntervals(
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
    }

    return builder.build()
  }

  @Test
  fun `generateEventBatches handles empty event group list`(): Unit = runBlocking {
    val eventGroupDetailsList = emptyList<GroupedRequisitions.EventGroupDetails>()

    val mockFactory = MockEventReaderFactory.withDefault(MockEventReader.empty())

    val eventSource = StorageEventSource(
      pathResolver = createMockPathResolver(),
      eventReaderFactory = mockFactory,
      eventGroupDetailsList = eventGroupDetailsList
    )

    val batches = eventSource.generateEventBatches(Dispatchers.Unconfined).toList()

    assertThat(batches).isEmpty()
  }

  @Test
  fun `secondary constructor creates default path resolver internally`(): Unit = runBlocking {
    val eventGroupDetailsList = listOf(
      createEventGroupDetails("event-group-1", LocalDate.of(2025, 1, 1), LocalDate.of(2025, 1, 1))
    )

    val eventSource = StorageEventSource(
      kmsClient = mockKmsClient,
      impressionsStorageConfig = mockStorageConfig,
      impressionDekStorageConfig = mockStorageConfig,
      impressionsBucketUri = "test-bucket",
      impressionsDekBucketUri = "test-dek-bucket",
      typeRegistry = mockTypeRegistry,
      eventGroupDetailsList = eventGroupDetailsList,
      batchSize = 10
    )

    assertThat(eventSource).isNotNull()
  }

  @Test
  fun `generateEventBatches fails when path resolution throws exception`(): Unit = runBlocking {
    val eventGroupDetailsList = listOf(
      createEventGroupDetails("event-group-1", LocalDate.of(2025, 1, 1), LocalDate.of(2025, 1, 1))
    )

    // Create path resolver that throws exception
    val failingPathResolver = object : EventPathResolver {
      override suspend fun resolvePaths(date: LocalDate, eventGroupReferenceId: String): EventPathResolver.EventPaths {
        throw RuntimeException("Path resolution failed")
      }
    }

    val mockFactory = MockEventReaderFactory.withDefault(MockEventReader.empty())

    val eventSource = StorageEventSource(
      pathResolver = failingPathResolver,
      eventReaderFactory = mockFactory,
      eventGroupDetailsList = eventGroupDetailsList
    )

    // The entire operation should fail when path resolution fails
    assertFailsWith<RuntimeException> {
      eventSource.generateEventBatches(Dispatchers.Unconfined).toList()
    }
  }

  @Test
  fun `generateEventBatches deduplicates EventReaders for overlapping intervals`(): Unit = runBlocking {
    // Create an event group with overlapping intervals
    // Interval 1: Jan 1-3 (3 days), Interval 2: Jan 2-4 (3 days)
    // Total dates without dedup: 6, unique dates with dedup: 4 (Jan 1, 2, 3, 4)
    val eventGroupDetailsList = listOf(
      createEventGroupDetailsWithMultipleIntervals(
        "event-group-1",
        listOf(
          Pair(LocalDate.of(2025, 1, 1), LocalDate.of(2025, 1, 3)), // Jan 1, 2, 3
          Pair(LocalDate.of(2025, 1, 2), LocalDate.of(2025, 1, 4))  // Jan 2, 3, 4 (Jan 2, 3 overlap)
        )
      )
    )

    // Create tracking path resolver
    // Each EventReader returns 2 events per batch
    val mockFactory = MockEventReaderFactory.withDefault(MockEventReader.withEventCount(2))

    val eventSource = StorageEventSource(
      pathResolver = createMockPathResolver(),
      eventReaderFactory = mockFactory,
      eventGroupDetailsList = eventGroupDetailsList
    )

    val batches = eventSource.generateEventBatches(Dispatchers.Unconfined).toList()

    // Verify the total number of events emitted
    val totalEvents = batches.flatMap { it.events }.size
    // With deduplication: 4 unique dates × 2 events per date = 8 events
    // Without deduplication: would be 6 dates × 2 events = 12 events
    assertThat(totalEvents).isEqualTo(8)

    // Verify we have exactly 4 batches (one per unique date)
    assertThat(batches).hasSize(4)

    // Verify each batch contains the expected number of events
    assertThat(batches.all { it.events.size == 2 }).isTrue()
  }

  // Test expandEventGroupDateRanges function
  @Test
  fun `expandEventGroupDateRanges handles empty list`() {
    val result = StorageEventSource.expandEventGroupDateRanges(emptyList())
    assertThat(result).isEmpty()
  }

  @Test
  fun `expandEventGroupDateRanges expands single day interval`() {
    val eventGroupDetailsList = listOf(
      createEventGroupDetails("group-1", LocalDate.of(2025, 1, 15), LocalDate.of(2025, 1, 15))
    )

    val result = StorageEventSource.expandEventGroupDateRanges(eventGroupDetailsList)

    assertThat(result).hasSize(1)
    assertThat(result[0].eventGroupReferenceId).isEqualTo("group-1")
    assertThat(result[0].startDate).isEqualTo(LocalDate.of(2025, 1, 15))
    assertThat(result[0].endDate).isEqualTo(LocalDate.of(2025, 1, 15))
  }

  @Test
  fun `expandEventGroupDateRanges expands multi-day interval`() {
    val eventGroupDetailsList = listOf(
      createEventGroupDetails("group-1", LocalDate.of(2025, 1, 1), LocalDate.of(2025, 1, 3))
    )

    val result = StorageEventSource.expandEventGroupDateRanges(eventGroupDetailsList)

    assertThat(result).hasSize(3)
    assertThat(result.map { it.startDate }).containsExactly(
      LocalDate.of(2025, 1, 1),
      LocalDate.of(2025, 1, 2),
      LocalDate.of(2025, 1, 3)
    )
    assertThat(result.all { it.eventGroupReferenceId == "group-1" }).isTrue()
  }

  @Test
  fun `expandEventGroupDateRanges deduplicates overlapping intervals`() {
    val eventGroupDetailsList = listOf(
      createEventGroupDetailsWithMultipleIntervals(
        "group-1",
        listOf(
          Pair(LocalDate.of(2025, 1, 1), LocalDate.of(2025, 1, 3)), // Jan 1, 2, 3
          Pair(LocalDate.of(2025, 1, 2), LocalDate.of(2025, 1, 4))  // Jan 2, 3, 4 (overlap on 2, 3)
        )
      )
    )

    val result = StorageEventSource.expandEventGroupDateRanges(eventGroupDetailsList)

    // Should have 4 unique dates, not 6
    assertThat(result).hasSize(4)
    assertThat(result.map { it.startDate }).containsExactly(
      LocalDate.of(2025, 1, 1),
      LocalDate.of(2025, 1, 2),
      LocalDate.of(2025, 1, 3),
      LocalDate.of(2025, 1, 4)
    )
  }

  @Test
  fun `expandEventGroupDateRanges handles multiple event groups`() {
    val eventGroupDetailsList = listOf(
      createEventGroupDetails("group-1", LocalDate.of(2025, 1, 1), LocalDate.of(2025, 1, 2)),
      createEventGroupDetails("group-2", LocalDate.of(2025, 1, 1), LocalDate.of(2025, 1, 1))
    )

    val result = StorageEventSource.expandEventGroupDateRanges(eventGroupDetailsList)

    assertThat(result).hasSize(3) // group-1: Jan 1,2 + group-2: Jan 1 = 3 total
    assertThat(result.filter { it.eventGroupReferenceId == "group-1" }).hasSize(2)
    assertThat(result.filter { it.eventGroupReferenceId == "group-2" }).hasSize(1)
  }

  // Test using MockEventReader for realistic scenarios
  @Test
  fun `generateEventBatches works with MockEventReader`(): Unit = runBlocking {
    val mockFactory = MockEventReaderFactory.withDefault(MockEventReader.withEventCount(5))
    val eventGroupDetails = createEventGroupDetails("group-1", LocalDate.of(2025, 1, 1), LocalDate.of(2025, 1, 1))

    val eventSource = StorageEventSource(
      pathResolver = createMockPathResolver(),
      eventReaderFactory = mockFactory,
      eventGroupDetailsList = listOf(eventGroupDetails)
    )

    val batches = eventSource.generateEventBatches(Dispatchers.Unconfined).toList()

    assertThat(batches).hasSize(1)
    assertThat(batches[0].events).hasSize(5)
    assertThat(batches[0].batchId).isEqualTo(1L)
  }

  @Test
  fun `generateEventBatches assigns unique batch IDs`(): Unit = runBlocking {
    val mockFactory = MockEventReaderFactory.withDefault(MockEventReader.withEventCount(2))
    val eventGroupDetails = createEventGroupDetails("group-1", LocalDate.of(2025, 1, 1), LocalDate.of(2025, 1, 2))

    val eventSource = StorageEventSource(
      pathResolver = createMockPathResolver(),
      eventReaderFactory = mockFactory,
      eventGroupDetailsList = listOf(eventGroupDetails)
    )

    val batches = eventSource.generateEventBatches(Dispatchers.Unconfined).toList()

    assertThat(batches).hasSize(2) // Two dates = two batches
    assertThat(batches.map { it.batchId }).containsNoDuplicates()
    assertThat(batches.map { it.batchId }).containsExactly(1L, 2L)
  }

  @Test
  fun `generateEventBatches handles EventReader exception`(): Unit = runBlocking {
    val mockFactory = MockEventReaderFactory.withDefault(
      MockEventReader.withException(RuntimeException("Event reading failed"))
    )
    val eventGroupDetails = createEventGroupDetails("group-1", LocalDate.of(2025, 1, 1), LocalDate.of(2025, 1, 1))

    val eventSource = StorageEventSource(
      pathResolver = createMockPathResolver(),
      eventReaderFactory = mockFactory,
      eventGroupDetailsList = listOf(eventGroupDetails)
    )

    assertFailsWith<RuntimeException> {
      eventSource.generateEventBatches(Dispatchers.Unconfined).toList()
    }
  }

  @Test
  fun `generateEventBatches handles multiple batches from single EventReader`(): Unit = runBlocking {
    val mockFactory = MockEventReaderFactory.withDefault(
      MockEventReader.withBatches(3, 2, 1) // Three batches with different sizes
    )
    val eventGroupDetails = createEventGroupDetails("group-1", LocalDate.of(2025, 1, 1), LocalDate.of(2025, 1, 1))

    val eventSource = StorageEventSource(
      pathResolver = createMockPathResolver(),
      eventReaderFactory = mockFactory,
      eventGroupDetailsList = listOf(eventGroupDetails)
    )

    val batches = eventSource.generateEventBatches(Dispatchers.Unconfined).toList()

    assertThat(batches).hasSize(3) // Three batches from single EventReader
    assertThat(batches[0].events).hasSize(3)
    assertThat(batches[1].events).hasSize(2)
    assertThat(batches[2].events).hasSize(1)
    assertThat(batches.map { it.batchId }).containsExactly(1L, 2L, 3L)
  }

  @Test
  fun `generateEventBatches uses path-specific EventReaders`(): Unit = runBlocking {
    val eventGroupDetails = listOf(
      createEventGroupDetails("group-1", LocalDate.of(2025, 1, 1), LocalDate.of(2025, 1, 1)),
      createEventGroupDetails("group-2", LocalDate.of(2025, 1, 1), LocalDate.of(2025, 1, 1))
    )

    // Create different readers for different paths
    val mappings = mapOf(
      Pair("blob-2025-01-01-group-1", "meta-2025-01-01-group-1") to MockEventReader.withEventCount(5),
      Pair("blob-2025-01-01-group-2", "meta-2025-01-01-group-2") to MockEventReader.withEventCount(3)
    )
    val mockFactory = MockEventReaderFactory.withMappings(mappings)

    val eventSource = StorageEventSource(
      pathResolver = createMockPathResolver(),
      eventReaderFactory = mockFactory,
      eventGroupDetailsList = eventGroupDetails
    )

    val batches = eventSource.generateEventBatches(Dispatchers.Unconfined).toList()

    assertThat(batches).hasSize(2) // Two event groups
    // Sort by event count to make assertions deterministic
    val sortedBatches = batches.sortedBy { it.events.size }
    assertThat(sortedBatches[0].events).hasSize(3) // group-2
    assertThat(sortedBatches[1].events).hasSize(5) // group-1
  }

  @Test
  fun `generateEventBatches processes large date ranges efficiently`(): Unit = runBlocking {
    // Test with a 10-day range to ensure it handles multiple dates
    val eventGroupDetails = createEventGroupDetails(
      "group-1",
      LocalDate.of(2025, 1, 1),
      LocalDate.of(2025, 1, 10)
    )

    val mockFactory = MockEventReaderFactory.withDefault(MockEventReader.withEventCount(2))

    val eventSource = StorageEventSource(
      pathResolver = createMockPathResolver(),
      eventReaderFactory = mockFactory,
      eventGroupDetailsList = listOf(eventGroupDetails)
    )

    val batches = eventSource.generateEventBatches(Dispatchers.Unconfined).toList()

    assertThat(batches).hasSize(10) // 10 days = 10 batches
    assertThat(batches.all { it.events.size == 2 }).isTrue() // Each batch has 2 events
    assertThat(batches.map { it.batchId }).containsExactly(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L)
  }
}
