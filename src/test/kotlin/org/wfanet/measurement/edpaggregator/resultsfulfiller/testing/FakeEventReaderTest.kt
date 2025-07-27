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

package org.wfanet.measurement.edpaggregator.resultsfulfiller.testing

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.timestamp
import com.google.type.interval
import java.time.LocalDate
import java.time.ZoneId
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.blobDetails
import kotlinx.coroutines.Dispatchers
import org.wfanet.measurement.edpaggregator.resultsfulfiller.EventPathResolver
import org.wfanet.measurement.edpaggregator.resultsfulfiller.StorageEventSource

@RunWith(JUnit4::class)
class FakeEventReaderTest {

  @Test
  fun `FakeEventReader empty returns no events`(): Unit = runBlocking {
    val fakeReader = FakeEventReader.empty()
    val batches = fakeReader.readEvents().toList()
    assertThat(batches).isEmpty()
  }

  @Test
  fun `FakeEventReader withEventCount returns correct number of events`(): Unit = runBlocking {
    val fakeReader = FakeEventReader.withEventCount(3)
    val batches = fakeReader.readEvents().toList()
    
    assertThat(batches).hasSize(1)
    assertThat(batches[0]).hasSize(3)
    assertThat(batches[0].map { it.eventGroupReferenceId }).containsExactly(
      "test-group-event-1",
      "test-group-event-2", 
      "test-group-event-3"
    )
  }

  @Test
  fun `FakeEventReader withBatches returns multiple batches`(): Unit = runBlocking {
    val fakeReader = FakeEventReader.withBatches(2, 3, 1)
    val batches = fakeReader.readEvents().toList()
    
    assertThat(batches).hasSize(3)
    assertThat(batches[0]).hasSize(2) // First batch: 2 events
    assertThat(batches[1]).hasSize(3) // Second batch: 3 events
    assertThat(batches[2]).hasSize(1) // Third batch: 1 event
  }

  @Test
  fun `FakeEventReader withException throws exception`(): Unit = runBlocking {
    val fakeReader = FakeEventReader.withException(RuntimeException("Test error"))
    
    try {
      fakeReader.readEvents().toList()
      assert(false) { "Expected exception to be thrown" }
    } catch (e: RuntimeException) {
      assertThat(e.message).isEqualTo("Test error")
    }
  }

  @Test
  fun `FakeEventReaderFactory withDefault returns same reader for all blob details`() {
    val fakeReader = FakeEventReader.withEventCount(5)
    val factory = FakeEventReaderFactory.withDefault(fakeReader)
    val bd1 = blobDetails { blobUri = "path1/metadata"; encryptedDek = EncryptedDek.getDefaultInstance() }
    val bd2 = blobDetails { blobUri = "path2/metadata"; encryptedDek = EncryptedDek.getDefaultInstance() }

    val reader1 = factory.createEventReader(bd1)
    val reader2 = factory.createEventReader(bd2)
    
    assertThat(reader1).isSameInstanceAs(fakeReader)
    assertThat(reader2).isSameInstanceAs(fakeReader)
  }

  @Test
  fun `FakeEventReaderFactory withMappings returns blob-specific readers`() {
    val reader1 = FakeEventReader.withEventCount(1)
    val reader2 = FakeEventReader.withEventCount(2)
    
    val mappings = mapOf(
      "meta1" to reader1,
      "meta2" to reader2
    )
    val factory = FakeEventReaderFactory.withMappings(mappings)
    val bd1 = blobDetails { blobUri = "meta1"; encryptedDek = EncryptedDek.getDefaultInstance() }
    val bd2 = blobDetails { blobUri = "meta2"; encryptedDek = EncryptedDek.getDefaultInstance() }
    val bd3 = blobDetails { blobUri = "unknown"; encryptedDek = EncryptedDek.getDefaultInstance() }

    assertThat(factory.createEventReader(bd1)).isSameInstanceAs(reader1)
    assertThat(factory.createEventReader(bd2)).isSameInstanceAs(reader2)
    assertThat(factory.createEventReader(bd3)).isInstanceOf(FakeEventReader::class.java)
  }

  @Test
  fun `StorageEventSource works with FakeEventReaderFactory`(): Unit = runBlocking {
    val fakePathResolver = object : EventPathResolver {
      override suspend fun resolvePaths(date: LocalDate, eventGroupReferenceId: String): EventPathResolver.EventPaths {
        return EventPathResolver.EventPaths("meta-$date", eventGroupReferenceId)
      }
    }

    val fakeReader = FakeEventReader.withEventCount(2)
    val fakeFactory = FakeEventReaderFactory.withDefault(fakeReader)

    val eventGroupDetails = createEventGroupDetails("group-1", LocalDate.of(2025, 1, 1), LocalDate.of(2025, 1, 2))
    
    val impressionService =
      PathEchoImpressionMetadataService(fakePathResolver, ZoneId.of("UTC"))
    val eventSource = StorageEventSource(
      impressionMetadataService = impressionService,
      eventReaderFactory = fakeFactory,
      eventGroupDetailsList = listOf(eventGroupDetails),
      zoneId = ZoneId.of("UTC")
    )

    val batches = eventSource.generateEventBatches(Dispatchers.Unconfined).toList()

    assertThat(batches).hasSize(2)  // 2 batches, one for each day
    assertThat(batches[0].events).hasSize(2)
    assertThat(batches[1].events).hasSize(2)
  }

  private fun createEventGroupDetails(eventGroupReferenceId: String, startDate: LocalDate, endDate: LocalDate): GroupedRequisitions.EventGroupDetails {
    val startInstant = startDate.atStartOfDay().toInstant(java.time.ZoneOffset.UTC)
    val endInstant = endDate.plusDays(1).atStartOfDay().toInstant(java.time.ZoneOffset.UTC).minusSeconds(1)
    
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
