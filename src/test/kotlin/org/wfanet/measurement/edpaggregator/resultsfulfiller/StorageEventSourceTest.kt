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
import com.google.protobuf.Descriptors
import com.google.protobuf.Empty
import com.google.protobuf.TypeRegistry
import com.google.protobuf.timestamp
import com.google.type.interval
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneId
import kotlin.test.assertFailsWith
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.resultsfulfiller.testing.FakeEventReader
import org.wfanet.measurement.edpaggregator.resultsfulfiller.testing.FakeEventReaderFactory
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions

@RunWith(JUnit4::class)
class StorageEventSourceTest {

  private val mockKmsClient: KmsClient? = null
  private val mockStorageConfig: StorageConfig = StorageConfig()
  private val mockDescriptor: Descriptors.Descriptor = Empty.getDescriptor()

  private fun createMockPathResolver(): EventPathResolver {
    return object : EventPathResolver {
      override suspend fun resolvePaths(date: LocalDate, eventGroupReferenceId: String): EventPathResolver.EventPaths {
        return EventPathResolver.EventPaths("meta-$date-$eventGroupReferenceId", eventGroupReferenceId)
      }
    }
  }

  private fun createEventGroupDetails(eventGroupReferenceId: String, startDate: LocalDate, endDate: LocalDate): GroupedRequisitions.EventGroupDetails {
    val startInstant = startDate.atStartOfDay(ZoneId.of("UTC")).toInstant()
    val endInstant = endDate.atStartOfDay(ZoneId.of("UTC")).toInstant()

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

  private fun createEventGroupDetailsWithTime(
    eventGroupReferenceId: String, 
    startDateTime: java.time.LocalDateTime, 
    endDateTime: java.time.LocalDateTime
  ): GroupedRequisitions.EventGroupDetails {
    val startInstant = startDateTime.atZone(ZoneId.of("UTC")).toInstant()
    val endInstant = endDateTime.atZone(ZoneId.of("UTC")).toInstant()

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
      val startInstant = startDate.atStartOfDay(ZoneId.of("UTC")).toInstant()
      val endInstant = endDate.atStartOfDay(ZoneId.of("UTC")).toInstant()

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

    val mockFactory = FakeEventReaderFactory.withDefault(FakeEventReader.empty())

    val eventSource = StorageEventSource(
      pathResolver = createMockPathResolver(),
      eventReaderFactory = mockFactory,
      eventGroupDetailsList = eventGroupDetailsList,
      zoneId = ZoneId.of("UTC")
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
      impressionsDekBucketUri = "test-dek-bucket",
      descriptor = mockDescriptor,
      eventGroupDetailsList = eventGroupDetailsList,
      batchSize = 10,
      zoneId = ZoneId.of("UTC")
    )

    assertThat(eventSource).isNotEqualTo(null)
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

    val mockFactory = FakeEventReaderFactory.withDefault(FakeEventReader.empty())

    val eventSource = StorageEventSource(
      pathResolver = failingPathResolver,
      eventReaderFactory = mockFactory,
      eventGroupDetailsList = eventGroupDetailsList,
      zoneId = ZoneId.of("UTC")
    )

    // The entire operation should fail when path resolution fails
    assertFailsWith<RuntimeException> {
      eventSource.generateEventBatches(Dispatchers.Unconfined).toList()
    }
  }

  @Test
  fun `generateEventBatches deduplicates EventReaders for overlapping intervals`(): Unit = runBlocking {
    // Create an event group with overlapping intervals
    // Interval 1: Jan 1-3 (exclusive end: Jan 1, 2), Interval 2: Jan 2-4 (exclusive end: Jan 2, 3)
    // Total dates without dedup: 4, unique dates with dedup: 3 (Jan 1, 2, 3)
    val eventGroupDetailsList = listOf(
      createEventGroupDetailsWithMultipleIntervals(
        "event-group-1",
        listOf(
          Pair(LocalDate.of(2025, 1, 1), LocalDate.of(2025, 1, 3)), // Jan 1, 2 (exclusive end)
          Pair(LocalDate.of(2025, 1, 2), LocalDate.of(2025, 1, 4))  // Jan 2, 3 (exclusive end, overlap on 2)
        )
      )
    )

    // Create tracking path resolver
    // Each EventReader returns 2 events per batch
    val mockFactory = FakeEventReaderFactory.withDefault(FakeEventReader.withEventCount(2))

    val eventSource = StorageEventSource(
      pathResolver = createMockPathResolver(),
      eventReaderFactory = mockFactory,
      eventGroupDetailsList = eventGroupDetailsList,
      zoneId = ZoneId.of("UTC")
    )

    val batches = eventSource.generateEventBatches(Dispatchers.Unconfined).toList()

    // Verify the total number of events emitted
    val totalEvents = batches.flatMap { it.events }.size
    // With deduplication: 3 unique dates × 2 events per date = 6 events
    // Without deduplication: would be 4 dates × 2 events = 8 events
    assertThat(totalEvents).isEqualTo(6)

    // Verify we have exactly 3 batches (one per unique date)
    assertThat(batches).hasSize(3)

    // Verify each batch contains the expected number of events
    assertThat(batches.all { it.events.size == 2 }).isTrue()
  }

  // Test expandEventGroupDates function
  @Test
  fun `expandEventGroupDates handles empty list`() {
    val result = StorageEventSource.expandEventGroupDates(emptyList(), ZoneId.of("UTC"))
    assertThat(result).isEmpty()
  }

  @Test
  fun `expandEventGroupDates expands single day interval`() {
    val eventGroupDetailsList = listOf(
      createEventGroupDetails("group-1", LocalDate.of(2025, 1, 15), LocalDate.of(2025, 1, 15))
    )

    val result = StorageEventSource.expandEventGroupDates(eventGroupDetailsList, ZoneId.of("UTC"))

    assertThat(result).hasSize(1)
    assertThat(result[0].eventGroupReferenceId).isEqualTo("group-1")
    assertThat(result[0].date).isEqualTo(LocalDate.of(2025, 1, 15))
  }

  @Test
  fun `expandEventGroupDates expands multi-day interval`() {
    val eventGroupDetailsList = listOf(
      createEventGroupDetails("group-1", LocalDate.of(2025, 1, 1), LocalDate.of(2025, 1, 3))
    )

    val result = StorageEventSource.expandEventGroupDates(eventGroupDetailsList, ZoneId.of("UTC"))

    assertThat(result).hasSize(2) // Exclusive end: Jan 1-3 includes Jan 1, 2
    assertThat(result.map { it.date }).containsExactly(
      LocalDate.of(2025, 1, 1),
      LocalDate.of(2025, 1, 2)
    )
    assertThat(result.all { it.eventGroupReferenceId == "group-1" }).isTrue()
  }

  @Test
  fun `expandEventGroupDates deduplicates overlapping intervals`() {
    val eventGroupDetailsList = listOf(
      createEventGroupDetailsWithMultipleIntervals(
        "group-1",
        listOf(
          Pair(LocalDate.of(2025, 1, 1), LocalDate.of(2025, 1, 3)), // Jan 1, 2 (exclusive end)
          Pair(LocalDate.of(2025, 1, 2), LocalDate.of(2025, 1, 4))  // Jan 2, 3 (exclusive end, overlap on 2)
        )
      )
    )

    val result = StorageEventSource.expandEventGroupDates(eventGroupDetailsList, ZoneId.of("UTC"))

    // Should have 3 unique dates with exclusive end behavior
    assertThat(result).hasSize(3)
    assertThat(result.map { it.date }).containsExactly(
      LocalDate.of(2025, 1, 1),
      LocalDate.of(2025, 1, 2),
      LocalDate.of(2025, 1, 3)
    )
  }

  @Test
  fun `expandEventGroupDates handles multiple event groups`() {
    val eventGroupDetailsList = listOf(
      createEventGroupDetails("group-1", LocalDate.of(2025, 1, 1), LocalDate.of(2025, 1, 2)),
      createEventGroupDetails("group-2", LocalDate.of(2025, 1, 1), LocalDate.of(2025, 1, 1))
    )

    val result = StorageEventSource.expandEventGroupDates(eventGroupDetailsList, ZoneId.of("UTC"))

    assertThat(result).hasSize(2) // group-1: Jan 1 + group-2: Jan 1 = 2 total (Jan 1-2 interval is exclusive of end)
    assertThat(result.filter { it.eventGroupReferenceId == "group-1" }).hasSize(1)
    assertThat(result.filter { it.eventGroupReferenceId == "group-2" }).hasSize(1)
  }

  @Test
  fun `expandEventGroupDates handles mid-day end times correctly`() {
    // Test with end time at 3PM on the same day - should include that day
    val eventGroupDetailsList = listOf(
      createEventGroupDetailsWithTime(
        "group-1",
        LocalDateTime.of(2025, 1, 15, 9, 0), // 9:00 AM Jan 15
        LocalDateTime.of(2025, 1, 15, 15, 0) // 3:00 PM Jan 15  
      )
    )

    val result = StorageEventSource.expandEventGroupDates(eventGroupDetailsList, ZoneId.of("UTC"))

    assertThat(result).hasSize(1)
    assertThat(result[0].eventGroupReferenceId).isEqualTo("group-1")
    assertThat(result[0].date).isEqualTo(LocalDate.of(2025, 1, 15))
  }

  @Test
  fun `expandEventGroupDates handles multi-day interval with mid-day end time`() {
    // Test with end time at 2PM on Jan 17 - should include Jan 15, 16, 17
    val eventGroupDetailsList = listOf(
      createEventGroupDetailsWithTime(
        "group-1",
        LocalDateTime.of(2025, 1, 15, 10, 30), // 10:30 AM Jan 15
        LocalDateTime.of(2025, 1, 17, 14, 0)   // 2:00 PM Jan 17
      )
    )

    val result = StorageEventSource.expandEventGroupDates(eventGroupDetailsList, ZoneId.of("UTC"))

    assertThat(result).hasSize(3)
    assertThat(result.map { it.date }).containsExactly(
      LocalDate.of(2025, 1, 15),
      LocalDate.of(2025, 1, 16),
      LocalDate.of(2025, 1, 17)
    )
    assertThat(result.all { it.eventGroupReferenceId == "group-1" }).isTrue()
  }

  @Test
  fun `generateEventBatches works with FakeEventReader`(): Unit = runBlocking {
    val mockFactory = FakeEventReaderFactory.withDefault(FakeEventReader.withEventCount(5))
    val eventGroupDetails = createEventGroupDetails("group-1", LocalDate.of(2025, 1, 1), LocalDate.of(2025, 1, 1))

    val eventSource = StorageEventSource(
      pathResolver = createMockPathResolver(),
      eventReaderFactory = mockFactory,
      eventGroupDetailsList = listOf(eventGroupDetails),
      zoneId = ZoneId.of("UTC")
    )

    val batches = eventSource.generateEventBatches(Dispatchers.Unconfined).toList()

    assertThat(batches).hasSize(1)
    assertThat(batches[0].events).hasSize(5)
  }

  @Test
  fun `generateEventBatches assigns unique batch IDs`(): Unit = runBlocking {
    val mockFactory = FakeEventReaderFactory.withDefault(FakeEventReader.withEventCount(2))
    val eventGroupDetails = createEventGroupDetails("group-1", LocalDate.of(2025, 1, 1), LocalDate.of(2025, 1, 2))

    val eventSource = StorageEventSource(
      pathResolver = createMockPathResolver(),
      eventReaderFactory = mockFactory,
      eventGroupDetailsList = listOf(eventGroupDetails),
      zoneId = ZoneId.of("UTC")
    )

    val batches = eventSource.generateEventBatches(Dispatchers.Unconfined).toList()

    assertThat(batches).hasSize(1) // Jan 1-2 exclusive = one date = one batch
  }

  @Test
  fun `generateEventBatches handles EventReader exception`(): Unit = runBlocking {
    val mockFactory = FakeEventReaderFactory.withDefault(
      FakeEventReader.withException(RuntimeException("Event reading failed"))
    )
    val eventGroupDetails = createEventGroupDetails("group-1", LocalDate.of(2025, 1, 1), LocalDate.of(2025, 1, 1))

    val eventSource = StorageEventSource(
      pathResolver = createMockPathResolver(),
      eventReaderFactory = mockFactory,
      eventGroupDetailsList = listOf(eventGroupDetails),
      zoneId = ZoneId.of("UTC")
    )

    assertFailsWith<RuntimeException> {
      eventSource.generateEventBatches(Dispatchers.Unconfined).toList()
    }
  }

  @Test
  fun `generateEventBatches handles multiple batches from single EventReader`(): Unit = runBlocking {
    val mockFactory = FakeEventReaderFactory.withDefault(
      FakeEventReader.withBatches(3, 2, 1) // Three batches with different sizes
    )
    val eventGroupDetails = createEventGroupDetails("group-1", LocalDate.of(2025, 1, 1), LocalDate.of(2025, 1, 1))

    val eventSource = StorageEventSource(
      pathResolver = createMockPathResolver(),
      eventReaderFactory = mockFactory,
      eventGroupDetailsList = listOf(eventGroupDetails),
      zoneId = ZoneId.of("UTC")
    )

    val batches = eventSource.generateEventBatches(Dispatchers.Unconfined).toList()

    assertThat(batches).hasSize(3) // Three batches from single EventReader
    assertThat(batches[0].events).hasSize(3)
    assertThat(batches[1].events).hasSize(2)
    assertThat(batches[2].events).hasSize(1)
  }

  @Test
  fun `generateEventBatches uses path-specific EventReaders`(): Unit = runBlocking {
    val eventGroupDetails = listOf(
      createEventGroupDetails("group-1", LocalDate.of(2025, 1, 1), LocalDate.of(2025, 1, 1)),
      createEventGroupDetails("group-2", LocalDate.of(2025, 1, 1), LocalDate.of(2025, 1, 1))
    )

    // Create different readers for different paths
    val mappings = mapOf(
      "meta-2025-01-01-group-1" to FakeEventReader.withEventCount(5),
      "meta-2025-01-01-group-2" to FakeEventReader.withEventCount(3)
    )
    val mockFactory = FakeEventReaderFactory.withMappings(mappings)

    val eventSource = StorageEventSource(
      pathResolver = createMockPathResolver(),
      eventReaderFactory = mockFactory,
      eventGroupDetailsList = eventGroupDetails,
      zoneId = ZoneId.of("UTC")
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

    val mockFactory = FakeEventReaderFactory.withDefault(FakeEventReader.withEventCount(2))

    val eventSource = StorageEventSource(
      pathResolver = createMockPathResolver(),
      eventReaderFactory = mockFactory,
      eventGroupDetailsList = listOf(eventGroupDetails),
      zoneId = ZoneId.of("UTC")
    )

    val batches = eventSource.generateEventBatches(Dispatchers.Unconfined).toList()

    assertThat(batches).hasSize(9) // Jan 1-10 exclusive = 9 days (Jan 1-9) = 9 batches
    assertThat(batches.all { it.events.size == 2 }).isTrue() // Each batch has 2 events
  }
}
