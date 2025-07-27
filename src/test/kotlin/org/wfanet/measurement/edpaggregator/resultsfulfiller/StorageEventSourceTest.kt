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
import java.time.ZoneId
import kotlin.test.assertFailsWith
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.resultsfulfiller.testing.FakeEventReader
import org.wfanet.measurement.edpaggregator.resultsfulfiller.testing.FakeEventReaderFactory
import org.wfanet.measurement.edpaggregator.resultsfulfiller.testing.PathEchoImpressionMetadataService
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions

@RunWith(JUnit4::class)
class StorageEventSourceTest {

  private fun createMockPathResolver(): EventPathResolver {
    return object : EventPathResolver {
      override suspend fun resolvePaths(
        date: LocalDate,
        eventGroupReferenceId: String,
      ): EventPathResolver.EventPaths {
        return EventPathResolver.EventPaths(
          "meta-$date-$eventGroupReferenceId",
          eventGroupReferenceId,
        )
      }
    }
  }

  private fun createEventGroupDetails(
    eventGroupReferenceId: String,
    startDate: LocalDate,
    endDate: LocalDate,
    zoneId: ZoneId,
  ): GroupedRequisitions.EventGroupDetails {
    val startInstant = startDate.atStartOfDay(zoneId).toInstant()
    val endInstant = endDate.atStartOfDay(zoneId).toInstant()

    return GroupedRequisitions.EventGroupDetails.newBuilder()
      .apply {
        this.eventGroupReferenceId = eventGroupReferenceId
        addCollectionIntervals(
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
      .build()
  }

  private fun createEventGroupDetailsWithMultipleIntervals(
    eventGroupReferenceId: String,
    intervals: List<Pair<LocalDate, LocalDate>>,
  ): GroupedRequisitions.EventGroupDetails {
    return GroupedRequisitions.EventGroupDetails.newBuilder()
      .apply {
        this.eventGroupReferenceId = eventGroupReferenceId

        intervals.forEach { (startDate, endDate) ->
          val startInstant = startDate.atStartOfDay(ZoneId.of("UTC")).toInstant()
          val endInstant = endDate.atStartOfDay(ZoneId.of("UTC")).toInstant()

          addCollectionIntervals(
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
      }
      .build()
  }

  @Test
  fun `generateEventBatches handles empty event group list`(): Unit = runBlocking {
    val eventGroupDetailsList = emptyList<GroupedRequisitions.EventGroupDetails>()

    val mockFactory = FakeEventReaderFactory.withDefault(FakeEventReader.empty())

    val impressionService =
      PathEchoImpressionMetadataService(createMockPathResolver(), ZoneId.of("UTC"))
    val eventSource =
      StorageEventSource(
        impressionMetadataService = impressionService,
        eventReaderFactory = mockFactory,
        eventGroupDetailsList = eventGroupDetailsList,
        zoneId = ZoneId.of("UTC"),
      )

    val batches = eventSource.generateEventBatches(Dispatchers.Unconfined).toList()

    assertThat(batches).isEmpty()
  }

  // Convenience constructor test removed: only facade-based initialization is supported.

  @Test
  fun `generateEventBatches fails when path resolution throws exception`(): Unit = runBlocking {
    val eventGroupDetailsList =
      listOf(
        createEventGroupDetails(
          "event-group-1",
          LocalDate.of(2025, 1, 1),
          LocalDate.of(2025, 1, 3),
          ZoneId.of("UTC"),
        )
      )

    // Create path resolver that throws exception
    val failingPathResolver =
      object : EventPathResolver {
        override suspend fun resolvePaths(
          date: LocalDate,
          eventGroupReferenceId: String,
        ): EventPathResolver.EventPaths {
          throw RuntimeException("Path resolution failed")
        }
      }

    val mockFactory = FakeEventReaderFactory.withDefault(FakeEventReader.empty())

    val impressionService = PathEchoImpressionMetadataService(failingPathResolver, ZoneId.of("UTC"))
    val eventSource =
      StorageEventSource(
        impressionMetadataService = impressionService,
        eventReaderFactory = mockFactory,
        eventGroupDetailsList = eventGroupDetailsList,
        zoneId = ZoneId.of("UTC"),
      )

    // The entire operation should fail when path resolution fails
    assertFailsWith<RuntimeException> {
      eventSource.generateEventBatches(Dispatchers.Unconfined).toList()
    }
  }

  @Test
  fun `generateEventBatches deduplicates EventReaders for overlapping intervals`(): Unit =
    runBlocking {
      // Create an event group with overlapping intervals
      // Interval 1: Jan 1-3 (exclusive end: Jan 1, 2), Interval 2: Jan 2-4 (exclusive end: Jan 2,
      // 3)
      // Total dates without dedup: 4, unique dates with dedup: 3 (Jan 1, 2, 3)
      val eventGroupDetailsList =
        listOf(
          createEventGroupDetailsWithMultipleIntervals(
            "event-group-1",
            listOf(
              Pair(LocalDate.of(2025, 1, 1), LocalDate.of(2025, 1, 3)), // Jan 1, 2 (exclusive end)
              Pair(
                LocalDate.of(2025, 1, 2),
                LocalDate.of(2025, 1, 4),
              ), // Jan 2, 3 (exclusive end, overlap on 2)
            ),
          )
        )

      // Create tracking path resolver
      // Each EventReader returns 2 events per batch
      val mockFactory = FakeEventReaderFactory.withDefault(FakeEventReader.withEventCount(2))

      val impressionService =
        PathEchoImpressionMetadataService(createMockPathResolver(), ZoneId.of("UTC"))
      val eventSource =
        StorageEventSource(
          impressionMetadataService = impressionService,
          eventReaderFactory = mockFactory,
          eventGroupDetailsList = eventGroupDetailsList,
          zoneId = ZoneId.of("UTC"),
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

  @Test
  fun `generateEventBatches works with FakeEventReader`(): Unit = runBlocking {
    val mockFactory = FakeEventReaderFactory.withDefault(FakeEventReader.withEventCount(5))
    val eventGroupDetails =
      createEventGroupDetails(
        "group-1",
        LocalDate.of(2025, 1, 1),
        LocalDate.of(2025, 1, 3),
        ZoneId.of("UTC"),
      )

    val impressionService =
      PathEchoImpressionMetadataService(createMockPathResolver(), ZoneId.of("UTC"))
    val eventSource =
      StorageEventSource(
        impressionMetadataService = impressionService,
        eventReaderFactory = mockFactory,
        eventGroupDetailsList = listOf(eventGroupDetails),
        zoneId = ZoneId.of("UTC"),
      )

    val batches = eventSource.generateEventBatches(Dispatchers.Unconfined).toList()

    // Jan 1-3 exclusive end = 2 dates (Jan 1, Jan 2) = 2 batches
    assertThat(batches).hasSize(2)
    assertThat(batches[0].events).hasSize(5)
    assertThat(batches[1].events).hasSize(5)
  }

  @Test
  fun `generateEventBatches handles EventReader exception`(): Unit = runBlocking {
    val mockFactory =
      FakeEventReaderFactory.withDefault(
        FakeEventReader.withException(RuntimeException("Event reading failed"))
      )
    val eventGroupDetails =
      createEventGroupDetails(
        "group-1",
        LocalDate.of(2025, 1, 1),
        LocalDate.of(2025, 1, 3),
        ZoneId.of("UTC"),
      )

    val impressionService =
      PathEchoImpressionMetadataService(createMockPathResolver(), ZoneId.of("UTC"))
    val eventSource =
      StorageEventSource(
        impressionMetadataService = impressionService,
        eventReaderFactory = mockFactory,
        eventGroupDetailsList = listOf(eventGroupDetails),
        zoneId = ZoneId.of("UTC"),
      )

    assertFailsWith<RuntimeException> {
      eventSource.generateEventBatches(Dispatchers.Unconfined).toList()
    }
  }

  @Test
  fun `generateEventBatches handles multiple batches from single EventReader`(): Unit =
    runBlocking {
      val mockFactory =
        FakeEventReaderFactory.withDefault(
          FakeEventReader.withBatches(3, 2, 1) // Three batches with different sizes
        )
      val eventGroupDetails =
        createEventGroupDetails(
          "group-1",
          LocalDate.of(2025, 1, 1),
          LocalDate.of(2025, 1, 3),
          ZoneId.of("UTC"),
        )

      val impressionService =
        PathEchoImpressionMetadataService(createMockPathResolver(), ZoneId.of("UTC"))
      val eventSource =
        StorageEventSource(
          impressionMetadataService = impressionService,
          eventReaderFactory = mockFactory,
          eventGroupDetailsList = listOf(eventGroupDetails),
          zoneId = ZoneId.of("UTC"),
        )

      val batches = eventSource.generateEventBatches(Dispatchers.Unconfined).toList()

      assertThat(batches).hasSize(6) // Three batches per EventReader × 2 EventReaders (2 dates)
      assertThat(batches[0].events).hasSize(3)
      assertThat(batches[1].events).hasSize(2)
      assertThat(batches[2].events).hasSize(1)
    }

  @Test
  fun `generateEventBatches uses path-specific EventReaders`(): Unit = runBlocking {
    val eventGroupDetails =
      listOf(
        createEventGroupDetails(
          "group-1",
          LocalDate.of(2025, 1, 1),
          LocalDate.of(2025, 1, 3),
          ZoneId.of("UTC"),
        ),
        createEventGroupDetails(
          "group-2",
          LocalDate.of(2025, 1, 1),
          LocalDate.of(2025, 1, 3),
          ZoneId.of("UTC"),
        ),
      )

    // Create different readers for different paths
    val mappings =
      mapOf(
        "meta-2025-01-01-group-1" to FakeEventReader.withEventCount(5),
        "meta-2025-01-01-group-2" to FakeEventReader.withEventCount(3),
        "meta-2025-01-02-group-1" to FakeEventReader.withEventCount(4),
        "meta-2025-01-02-group-2" to FakeEventReader.withEventCount(2),
      )
    val mockFactory = FakeEventReaderFactory.withMappings(mappings)

    val impressionService =
      PathEchoImpressionMetadataService(createMockPathResolver(), ZoneId.of("UTC"))
    val eventSource =
      StorageEventSource(
        impressionMetadataService = impressionService,
        eventReaderFactory = mockFactory,
        eventGroupDetailsList = eventGroupDetails,
        zoneId = ZoneId.of("UTC"),
      )

    val batches = eventSource.generateEventBatches(Dispatchers.Unconfined).toList()

    assertThat(batches).hasSize(4) // Two event groups × two dates
    // Verify total events per group
    val totalEvents = batches.flatMap { it.events }.size
    assertThat(totalEvents).isEqualTo(14) // 5 + 3 + 4 + 2
  }

  @Test
  fun `generateEventBatches processes large date ranges efficiently`(): Unit = runBlocking {
    // Test with a 10-day range to ensure it handles multiple dates
    val eventGroupDetails =
      createEventGroupDetails(
        "group-1",
        LocalDate.of(2025, 1, 1),
        LocalDate.of(2025, 1, 10),
        ZoneId.of("UTC"),
      )

    val mockFactory = FakeEventReaderFactory.withDefault(FakeEventReader.withEventCount(2))

    val impressionService =
      PathEchoImpressionMetadataService(createMockPathResolver(), ZoneId.of("UTC"))
    val eventSource =
      StorageEventSource(
        impressionMetadataService = impressionService,
        eventReaderFactory = mockFactory,
        eventGroupDetailsList = listOf(eventGroupDetails),
        zoneId = ZoneId.of("UTC"),
      )

    val batches = eventSource.generateEventBatches(Dispatchers.Unconfined).toList()

    assertThat(batches).hasSize(9) // Jan 1-10 exclusive = 9 days (Jan 1-9) = 9 batches
    assertThat(batches.all { it.events.size == 2 }).isTrue() // Each batch has 2 events
  }
}
