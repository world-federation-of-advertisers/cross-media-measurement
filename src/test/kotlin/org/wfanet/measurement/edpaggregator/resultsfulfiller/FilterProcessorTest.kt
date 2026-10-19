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
import com.google.protobuf.Descriptors
import com.google.protobuf.DynamicMessage
import com.google.protobuf.Message
import com.google.protobuf.timestamp
import com.google.protobuf.util.Timestamps
import com.google.type.Interval
import com.google.type.interval
import java.time.Instant
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.edpaggregator.v1alpha.EntityKeyGroup
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpressionKt.entityKey
import org.wfanet.measurement.edpaggregator.v1alpha.entityKeyGroup

@RunWith(JUnit4::class)
class FilterProcessorTest {

  private val testEventDescriptor: Descriptors.Descriptor = TestEvent.getDescriptor()

  /** Helper function to create a DynamicMessage for a TestEvent. */
  private fun createDynamicMessage(
    ageGroup: Person.AgeGroup = Person.AgeGroup.YEARS_18_TO_34,
    gender: Person.Gender = Person.Gender.MALE,
  ): DynamicMessage {
    val personDescriptor = Person.getDescriptor()
    val ageGroupField = personDescriptor.findFieldByName("age_group")
    val genderField = personDescriptor.findFieldByName("gender")

    // Get EnumValueDescriptor for the enum values
    val ageGroupValueDescriptor = ageGroupField.enumType.findValueByNumber(ageGroup.number)
    val genderValueDescriptor = genderField.enumType.findValueByNumber(gender.number)

    return DynamicMessage.newBuilder(testEventDescriptor)
      .setField(
        testEventDescriptor.findFieldByName("person"),
        DynamicMessage.newBuilder(personDescriptor)
          .setField(ageGroupField, ageGroupValueDescriptor)
          .setField(genderField, genderValueDescriptor)
          .build(),
      )
      .build()
  }

  /** Helper function to create a LabeledEvent for testing. */
  private fun createTestLabeledEvent(
    vid: Long,
    entityKeys: List<LabeledImpression.EntityKey>,
    timestamp: Instant = Instant.now(),
    ageGroup: Person.AgeGroup = Person.AgeGroup.YEARS_18_TO_34,
    gender: Person.Gender = Person.Gender.MALE,
  ): LabeledEvent<Message> {
    val message = createDynamicMessage(ageGroup, gender)
    return LabeledEvent(
      timestamp = timestamp,
      vid = vid,
      message = message,
      entityKeys = entityKeys,
    )
  }

  /** Helper function to create an EventBatch with proper minTime and maxTime. */
  private fun createEventBatch(
    events: List<LabeledEvent<Message>>,
    eventGroupIdentifier: EventGroupIdentifier,
  ): EventBatch<Message> {
    val timestamps = events.map { it.timestamp }
    val minTime = timestamps.minOrNull() ?: Instant.now()
    val maxTime = timestamps.maxOrNull() ?: Instant.now()
    return EventBatch(events, minTime, maxTime, eventGroupIdentifier = eventGroupIdentifier)
  }

  /** Helper function to create a default interval that covers a wide time range. */
  private fun createDefaultInterval(): Interval = interval {
    startTime = timestamp { seconds = Instant.parse("2000-01-01T00:00:00Z").epochSecond }
    endTime = timestamp { seconds = Instant.parse("2050-12-31T23:59:59Z").epochSecond }
  }

  /** Helper function to create an invalid interval (endTime <= startTime). */
  private fun createInvalidInterval(): Interval = interval {
    startTime = timestamp { seconds = Instant.parse("2050-01-01T00:00:00Z").epochSecond }
    endTime = timestamp { seconds = Instant.parse("2000-01-01T00:00:00Z").epochSecond }
  }

  /** Helper function to create a FilterSpec for testing. */
  private fun createTestFilterSpec(
    celExpression: String = "",
    collectionInterval: Interval = createDefaultInterval(),
    eventGroupReferenceIds: Set<String> = setOf("test-group"),
    entityKeys: Set<LabeledImpression.EntityKey> = emptySet(),
  ): FilterSpec {
    return FilterSpec(
      celExpression = celExpression,
      collectionInterval = collectionInterval,
      eventGroupReferenceIds = eventGroupReferenceIds,
      entityKeys = entityKeys,
    )
  }

  /** Helper function to build a [LabeledImpression.EntityKey]. */
  private fun makeEntityKey(type: String, id: String): LabeledImpression.EntityKey = entityKey {
    entityType = type
    entityId = id
  }

  /** Helper function to build an [EntityKeyGroup] for a single (type, ids...) tuple. */
  private fun makeEntityKeyGroup(type: String, vararg ids: String): EntityKeyGroup =
    entityKeyGroup {
      entityType = type
      entityIds += ids.toList()
    }

  private fun unmappedEntityKeys(entityKeys: List<EntityKeyGroup>): EventGroupIdentifier {
    return entityKeyIdentifier(entityKeys, wholeBlobEntityTypes = emptySet())
  }

  private fun entityKeyIdentifier(
    entityKeys: List<EntityKeyGroup>,
    wholeBlobEntityTypes: Set<String>,
  ): EventGroupIdentifier {
    val flattenedEntityKeys: Set<LabeledImpression.EntityKey> =
      entityKeys
        .flatMap { group ->
          group.entityIdsList.map { entityId -> makeEntityKey(group.entityType, entityId) }
        }
        .toSet()
    return EventGroupIdentifier(
      eventGroupReferenceId = null,
      entityKeys = flattenedEntityKeys,
      wholeBlobEntityIdsByType =
        flattenedEntityKeys
          .asSequence()
          .filter { it.entityType in wholeBlobEntityTypes }
          .groupBy({ it.entityType }, { it.entityId })
          .mapValues { (_, entityIds) -> entityIds.toSet() },
    )
  }

  @Test
  fun `processBatch filters events with CEL expression`() {
    runBlocking {
      val celExpression = "person.age_group == 1" // YEARS_18_TO_34
      val filterSpec = createTestFilterSpec(celExpression = celExpression)
      val filterProcessor = FilterProcessor<Message>(filterSpec, testEventDescriptor)

      val events =
        listOf(
          createTestLabeledEvent(
            vid = 1,
            ageGroup = Person.AgeGroup.YEARS_18_TO_34,
            entityKeys = emptyList(),
          ),
          createTestLabeledEvent(
            vid = 2,
            ageGroup = Person.AgeGroup.YEARS_35_TO_54,
            entityKeys = emptyList(),
          ),
          createTestLabeledEvent(
            vid = 3,
            ageGroup = Person.AgeGroup.YEARS_18_TO_34,
            entityKeys = emptyList(),
          ),
        )

      val batch =
        createEventBatch(
          events,
          EventGroupIdentifier(
            eventGroupReferenceId = "test-group",
            entityKeys = emptySet(),
            wholeBlobEntityIdsByType = emptyMap(),
          ),
        )
      val result = filterProcessor.processBatch(batch)

      assertThat(result.events).hasSize(2)
      assertThat(result.events.map { it.vid }).containsExactly(1L, 3L)
    }
  }

  @Test
  fun `processBatch with empty CEL expression matches all events`() {
    runBlocking {
      val filterSpec = createTestFilterSpec(celExpression = "")
      val filterProcessor = FilterProcessor<Message>(filterSpec, testEventDescriptor)

      val events =
        listOf(
          createTestLabeledEvent(
            1,
            ageGroup = Person.AgeGroup.YEARS_18_TO_34,
            entityKeys = emptyList(),
          ),
          createTestLabeledEvent(
            2,
            ageGroup = Person.AgeGroup.YEARS_35_TO_54,
            entityKeys = emptyList(),
          ),
          createTestLabeledEvent(
            3,
            ageGroup = Person.AgeGroup.YEARS_55_PLUS,
            entityKeys = emptyList(),
          ),
        )

      val batch =
        createEventBatch(
          events,
          EventGroupIdentifier(
            eventGroupReferenceId = "test-group",
            entityKeys = emptySet(),
            wholeBlobEntityIdsByType = emptyMap(),
          ),
        )
      val result = filterProcessor.processBatch(batch)

      assertThat(result.events).hasSize(3)
      assertThat(result.events.map { it.vid }).containsExactly(1L, 2L, 3L)
    }
  }

  @Test
  fun `processBatch filters by time range`() {
    runBlocking {
      val startTime = Instant.parse("2025-01-01T00:00:00Z")
      val endTime = Instant.parse("2025-01-31T23:59:59Z")

      val interval =
        Interval.newBuilder()
          .setStartTime(Timestamps.fromMillis(startTime.toEpochMilli()))
          .setEndTime(Timestamps.fromMillis(endTime.toEpochMilli()))
          .build()

      val filterSpec = createTestFilterSpec(celExpression = "", collectionInterval = interval)
      val filterProcessor = FilterProcessor<Message>(filterSpec, testEventDescriptor)

      val events =
        listOf(
          createTestLabeledEvent(
            1,
            timestamp = Instant.parse("2024-12-31T23:59:59Z"),
            entityKeys = emptyList(),
          ), // Before range
          createTestLabeledEvent(
            2,
            timestamp = Instant.parse("2025-01-15T12:00:00Z"),
            entityKeys = emptyList(),
          ), // Within range
          createTestLabeledEvent(
            3,
            timestamp = Instant.parse("2025-02-01T00:00:00Z"),
            entityKeys = emptyList(),
          ), // After range
        )

      val batch =
        createEventBatch(
          events,
          EventGroupIdentifier(
            eventGroupReferenceId = "test-group",
            entityKeys = emptySet(),
            wholeBlobEntityIdsByType = emptyMap(),
          ),
        )
      val result = filterProcessor.processBatch(batch)

      // Only event 2 should match (within time range)
      assertThat(result.events).hasSize(1)
      assertThat(result.events[0].vid).isEqualTo(2L)
    }
  }

  @Test
  fun `processBatch filters by event group reference ID`() {
    runBlocking {
      val targetEventGroupId = "test-group" // Match the batch's eventGroupReferenceId

      val filterSpec =
        createTestFilterSpec(celExpression = "", eventGroupReferenceIds = setOf(targetEventGroupId))
      val filterProcessor = FilterProcessor<Message>(filterSpec, testEventDescriptor)

      val events =
        listOf(
          createTestLabeledEvent(1, entityKeys = emptyList()),
          createTestLabeledEvent(2, entityKeys = emptyList()),
          createTestLabeledEvent(3, entityKeys = emptyList()),
        )

      val batch =
        createEventBatch(
          events,
          EventGroupIdentifier(
            eventGroupReferenceId = "test-group",
            entityKeys = emptySet(),
            wholeBlobEntityIdsByType = emptyMap(),
          ),
        )
      val result = filterProcessor.processBatch(batch)

      // All events should pass since the batch's eventGroupReferenceId matches
      assertThat(result.events).hasSize(3)
      assertThat(result.events.map { it.vid }).containsExactly(1L, 2L, 3L)
    }
  }

  @Test
  fun `processBatch applies combined filters`() {
    runBlocking {
      val celExpression = "person.gender == 1" // MALE
      val targetEventGroupId = "test-group" // Match the batch's eventGroupReferenceId
      val startTime = Instant.parse("2025-01-01T00:00:00Z")
      val endTime = Instant.parse("2025-01-31T23:59:59Z")

      val interval =
        Interval.newBuilder()
          .setStartTime(Timestamps.fromMillis(startTime.toEpochMilli()))
          .setEndTime(Timestamps.fromMillis(endTime.toEpochMilli()))
          .build()

      val filterSpec =
        createTestFilterSpec(
          celExpression = celExpression,
          collectionInterval = interval,
          eventGroupReferenceIds = setOf(targetEventGroupId),
        )
      val filterProcessor = FilterProcessor<Message>(filterSpec, testEventDescriptor)

      val events =
        listOf(
          createTestLabeledEvent(
            1,
            gender = Person.Gender.MALE,
            timestamp = Instant.parse("2025-01-15T12:00:00Z"),
            entityKeys = emptyList(),
          ),
          createTestLabeledEvent(
            2,
            gender = Person.Gender.FEMALE,
            timestamp = Instant.parse("2025-01-15T12:00:00Z"),
            entityKeys = emptyList(),
          ),
          createTestLabeledEvent(
            3,
            gender = Person.Gender.MALE,
            timestamp = Instant.parse("2025-01-15T12:00:00Z"),
            entityKeys = emptyList(),
          ),
          createTestLabeledEvent(
            4,
            gender = Person.Gender.MALE,
            timestamp = Instant.parse("2024-12-31T23:59:59Z"),
            entityKeys = emptyList(),
          ),
        )

      val batch =
        createEventBatch(
          events,
          EventGroupIdentifier(
            eventGroupReferenceId = "test-group",
            entityKeys = emptySet(),
            wholeBlobEntityIdsByType = emptyMap(),
          ),
        )
      val result = filterProcessor.processBatch(batch)

      // Events 1 and 3 should match (male, within time range); event 4 is outside time range
      assertThat(result.events).hasSize(2)
      assertThat(result.events.map { it.vid }).containsExactly(1L, 3L)
    }
  }

  @Test
  fun `processBatch with complex CEL expression`() {
    runBlocking {
      val celExpression = "person.age_group == 1 && person.gender == 1" // YEARS_18_TO_34 && MALE
      val filterSpec = createTestFilterSpec(celExpression = celExpression)
      val filterProcessor = FilterProcessor<Message>(filterSpec, testEventDescriptor)

      val events =
        listOf(
          createTestLabeledEvent(
            1,
            ageGroup = Person.AgeGroup.YEARS_18_TO_34,
            gender = Person.Gender.MALE,
            entityKeys = emptyList(),
          ),
          createTestLabeledEvent(
            2,
            ageGroup = Person.AgeGroup.YEARS_18_TO_34,
            gender = Person.Gender.FEMALE,
            entityKeys = emptyList(),
          ),
          createTestLabeledEvent(
            3,
            ageGroup = Person.AgeGroup.YEARS_35_TO_54,
            gender = Person.Gender.MALE,
            entityKeys = emptyList(),
          ),
          createTestLabeledEvent(
            4,
            ageGroup = Person.AgeGroup.YEARS_18_TO_34,
            gender = Person.Gender.MALE,
            entityKeys = emptyList(),
          ),
        )

      val batch =
        createEventBatch(
          events,
          EventGroupIdentifier(
            eventGroupReferenceId = "test-group",
            entityKeys = emptySet(),
            wholeBlobEntityIdsByType = emptyMap(),
          ),
        )
      val result = filterProcessor.processBatch(batch)

      assertThat(result.events).hasSize(2)
      assertThat(result.events.map { it.vid }).containsExactly(1L, 4L)
    }
  }

  @Test
  fun `processBatch skips entire batch when no time overlap`() {
    runBlocking {
      val startTime = Instant.parse("2025-01-01T00:00:00Z")
      val endTime = Instant.parse("2025-01-31T23:59:59Z")

      val interval =
        Interval.newBuilder()
          .setStartTime(Timestamps.fromMillis(startTime.toEpochMilli()))
          .setEndTime(Timestamps.fromMillis(endTime.toEpochMilli()))
          .build()

      val filterSpec = createTestFilterSpec(celExpression = "", collectionInterval = interval)
      val filterProcessor = FilterProcessor<Message>(filterSpec, testEventDescriptor)

      // All events are outside the collection interval (before it starts)
      val events =
        listOf(
          createTestLabeledEvent(
            1,
            timestamp = Instant.parse("2024-12-01T00:00:00Z"),
            entityKeys = emptyList(),
          ),
          createTestLabeledEvent(
            2,
            timestamp = Instant.parse("2024-12-15T12:00:00Z"),
            entityKeys = emptyList(),
          ),
          createTestLabeledEvent(
            3,
            timestamp = Instant.parse("2024-12-31T23:59:59Z"),
            entityKeys = emptyList(),
          ),
        )

      val batch =
        createEventBatch(
          events,
          EventGroupIdentifier(
            eventGroupReferenceId = "test-group",
            entityKeys = emptySet(),
            wholeBlobEntityIdsByType = emptyMap(),
          ),
        )
      val result = filterProcessor.processBatch(batch)

      // Should return empty batch since no time overlap
      assertThat(result.events).isEmpty()
    }
  }

  @Test
  fun `processBatch processes batch when time ranges overlap`() {
    runBlocking {
      val startTime = Instant.parse("2025-01-01T00:00:00Z")
      val endTime = Instant.parse("2025-01-31T23:59:59Z")

      val interval =
        Interval.newBuilder()
          .setStartTime(Timestamps.fromMillis(startTime.toEpochMilli()))
          .setEndTime(Timestamps.fromMillis(endTime.toEpochMilli()))
          .build()

      val filterSpec = createTestFilterSpec(celExpression = "", collectionInterval = interval)
      val filterProcessor = FilterProcessor<Message>(filterSpec, testEventDescriptor)

      // Batch spans across filter interval (some events before, some during, some after)
      val events =
        listOf(
          createTestLabeledEvent(
            1,
            timestamp = Instant.parse("2024-12-31T23:59:59Z"),
            entityKeys = emptyList(),
          ), // Before interval
          createTestLabeledEvent(
            2,
            timestamp = Instant.parse("2025-01-15T12:00:00Z"),
            entityKeys = emptyList(),
          ), // Within interval
          createTestLabeledEvent(
            3,
            timestamp = Instant.parse("2025-02-01T00:00:00Z"),
            entityKeys = emptyList(),
          ), // After interval
        )

      val batch =
        createEventBatch(
          events,
          EventGroupIdentifier(
            eventGroupReferenceId = "test-group",
            entityKeys = emptySet(),
            wholeBlobEntityIdsByType = emptyMap(),
          ),
        )
      val result = filterProcessor.processBatch(batch)

      // Should process batch and filter individual events - only event 2 should match
      assertThat(result.events).hasSize(1)
      assertThat(result.events[0].vid).isEqualTo(2L)
    }
  }

  @Test
  fun `processBatch skips batch when entirely after filter interval`() {
    runBlocking {
      val startTime = Instant.parse("2025-01-01T00:00:00Z")
      val endTime = Instant.parse("2025-01-31T23:59:59Z")

      val interval =
        Interval.newBuilder()
          .setStartTime(Timestamps.fromMillis(startTime.toEpochMilli()))
          .setEndTime(Timestamps.fromMillis(endTime.toEpochMilli()))
          .build()

      val filterSpec = createTestFilterSpec(celExpression = "", collectionInterval = interval)
      val filterProcessor = FilterProcessor<Message>(filterSpec, testEventDescriptor)

      // All events are after the collection interval
      val events =
        listOf(
          createTestLabeledEvent(
            1,
            timestamp = Instant.parse("2025-02-01T00:00:00Z"),
            entityKeys = emptyList(),
          ),
          createTestLabeledEvent(
            2,
            timestamp = Instant.parse("2025-03-15T12:00:00Z"),
            entityKeys = emptyList(),
          ),
          createTestLabeledEvent(
            3,
            timestamp = Instant.parse("2025-04-01T00:00:00Z"),
            entityKeys = emptyList(),
          ),
        )

      val batch =
        createEventBatch(
          events,
          EventGroupIdentifier(
            eventGroupReferenceId = "test-group",
            entityKeys = emptySet(),
            wholeBlobEntityIdsByType = emptyMap(),
          ),
        )
      val result = filterProcessor.processBatch(batch)

      // Should return empty batch since no time overlap
      assertThat(result.events).isEmpty()
    }
  }

  @Test
  fun `processBatch filters events at time range boundaries`() {
    runBlocking {
      val startTime = Instant.parse("2025-01-01T00:00:00Z")
      val endTime = Instant.parse("2025-01-02T00:00:00Z")

      val interval =
        Interval.newBuilder()
          .setStartTime(Timestamps.fromMillis(startTime.toEpochMilli()))
          .setEndTime(Timestamps.fromMillis(endTime.toEpochMilli()))
          .build()

      val filterSpec = createTestFilterSpec(celExpression = "", collectionInterval = interval)
      val filterProcessor = FilterProcessor<Message>(filterSpec, testEventDescriptor)

      val events =
        listOf(
          createTestLabeledEvent(
            1,
            timestamp = Instant.parse("2024-12-31T23:59:59.999Z"),
            entityKeys = emptyList(),
          ), // Just before start
          createTestLabeledEvent(
            2,
            timestamp = startTime,
            entityKeys = emptyList(),
          ), // Exactly at start (inclusive)
          createTestLabeledEvent(
            3,
            timestamp = Instant.parse("2025-01-01T12:00:00Z"),
            entityKeys = emptyList(),
          ), // Within range
          createTestLabeledEvent(
            4,
            timestamp = endTime.minusMillis(1),
            entityKeys = emptyList(),
          ), // Just before end
          createTestLabeledEvent(
            5,
            timestamp = endTime,
            entityKeys = emptyList(),
          ), // Exactly at end (exclusive)
        )

      val batch =
        createEventBatch(
          events,
          EventGroupIdentifier(
            eventGroupReferenceId = "test-group",
            entityKeys = emptySet(),
            wholeBlobEntityIdsByType = emptyMap(),
          ),
        )
      val result = filterProcessor.processBatch(batch)

      // Events 2, 3, and 4 should match (start inclusive, end exclusive)
      assertThat(result.events).hasSize(3)
      assertThat(result.events.map { it.vid }).containsExactly(2L, 3L, 4L)
    }
  }

  @Test
  fun `processBatch with empty entityKeys filter passes all events`() {
    runBlocking {
      // Regression: existing behavior preserved when no entity-key filter is requested.
      val filterSpec = createTestFilterSpec()
      val filterProcessor = FilterProcessor<Message>(filterSpec, testEventDescriptor)

      val events =
        listOf(
          createTestLabeledEvent(1, entityKeys = listOf(makeEntityKey("ad", "X"))),
          createTestLabeledEvent(2, entityKeys = emptyList()),
          createTestLabeledEvent(3, entityKeys = listOf(makeEntityKey("placement", "P"))),
        )

      val result =
        filterProcessor.processBatch(
          createEventBatch(
            events,
            EventGroupIdentifier(
              eventGroupReferenceId = "test-group",
              entityKeys = emptySet(),
              wholeBlobEntityIdsByType = emptyMap(),
            ),
          )
        )

      assertThat(result.events.map { it.vid }).containsExactly(1L, 2L, 3L).inOrder()
    }
  }

  @Test
  fun `processBatch with entityKeys filter keeps events whose keys intersect`() {
    runBlocking {
      val targetKey = makeEntityKey("ad", "X")
      val filterSpec = createTestFilterSpec(entityKeys = setOf(targetKey))
      val filterProcessor = FilterProcessor<Message>(filterSpec, testEventDescriptor)

      val events =
        listOf(
          createTestLabeledEvent(1, entityKeys = listOf(targetKey)),
          createTestLabeledEvent(2, entityKeys = listOf(makeEntityKey("ad", "Y"), targetKey)),
        )

      val result =
        filterProcessor.processBatch(
          createEventBatch(events, unmappedEntityKeys(listOf(makeEntityKeyGroup("ad", "X", "Y"))))
        )

      assertThat(result.events.map { it.vid }).containsExactly(1L, 2L).inOrder()
    }
  }

  @Test
  fun `processBatch with entityKeys filter drops events whose keys do not intersect`() {
    runBlocking {
      val filterSpec = createTestFilterSpec(entityKeys = setOf(makeEntityKey("ad", "X")))
      val filterProcessor = FilterProcessor<Message>(filterSpec, testEventDescriptor)

      val events =
        listOf(
          createTestLabeledEvent(1, entityKeys = listOf(makeEntityKey("ad", "Y"))),
          createTestLabeledEvent(2, entityKeys = listOf(makeEntityKey("placement", "X"))),
        )

      val result =
        filterProcessor.processBatch(
          createEventBatch(
            events,
            unmappedEntityKeys(
              listOf(makeEntityKeyGroup("ad", "X", "Y"), makeEntityKeyGroup("placement", "X"))
            ),
          )
        )

      assertThat(result.events).isEmpty()
    }
  }

  @Test
  fun `processBatch throws EmptyEventGroupIdentifierException when batch has no selector`() {
    runBlocking {
      val filterSpec = createTestFilterSpec(entityKeys = setOf(makeEntityKey("ad", "X")))
      val filterProcessor = FilterProcessor<Message>(filterSpec, testEventDescriptor)

      val events = listOf(createTestLabeledEvent(1, entityKeys = emptyList()))

      assertFailsWith<EmptyEventGroupIdentifierException> {
        filterProcessor.processBatch(createEventBatch(events, unmappedEntityKeys(emptyList())))
      }
    }
  }

  @Test
  fun `processBatch returns empty when filter has entity keys and batch has only a reference ID`() {
    runBlocking {
      val filterSpec =
        FilterSpec(
          celExpression = "",
          collectionInterval = createDefaultInterval(),
          eventGroupReferenceIds = emptySet(),
          entityKeys = setOf(makeEntityKey("ad", "X")),
        )
      val filterProcessor = FilterProcessor<Message>(filterSpec, testEventDescriptor)

      val events = listOf(createTestLabeledEvent(1, entityKeys = listOf(makeEntityKey("ad", "X"))))
      val batch =
        EventBatch(
          events,
          minTime = events.minOf { it.timestamp },
          maxTime = events.maxOf { it.timestamp },
          eventGroupIdentifier =
            EventGroupIdentifier(
              eventGroupReferenceId = "some-ref-id",
              entityKeys = emptySet(),
              wholeBlobEntityIdsByType = emptyMap(),
            ),
        )

      val result = filterProcessor.processBatch(batch)
      assertThat(result.events).isEmpty()
    }
  }

  @Test
  fun `processBatch returns empty when filter has reference IDs and batch has only entity keys`() {
    runBlocking {
      val filterSpec = createTestFilterSpec(eventGroupReferenceIds = setOf("test-group"))
      val filterProcessor = FilterProcessor<Message>(filterSpec, testEventDescriptor)

      val events = listOf(createTestLabeledEvent(1, entityKeys = listOf(makeEntityKey("ad", "X"))))
      val batch =
        EventBatch(
          events,
          minTime = events.minOf { it.timestamp },
          maxTime = events.maxOf { it.timestamp },
          eventGroupIdentifier = unmappedEntityKeys(listOf(makeEntityKeyGroup("ad", "X"))),
        )

      val result = filterProcessor.processBatch(batch)
      assertThat(result.events).isEmpty()
    }
  }

  @Test
  fun `processBatch with entityKeys filter works when batch eventGroupReferenceId would be empty`() {
    runBlocking {
      val targetKey = makeEntityKey("ad", "X")
      val filterSpec = createTestFilterSpec(entityKeys = setOf(targetKey))
      val filterProcessor = FilterProcessor<Message>(filterSpec, testEventDescriptor)

      val events = listOf(createTestLabeledEvent(1, entityKeys = listOf(targetKey)))
      val batch =
        EventBatch(
          events,
          minTime = events.minOf { it.timestamp },
          maxTime = events.maxOf { it.timestamp },
          eventGroupIdentifier = unmappedEntityKeys(listOf(makeEntityKeyGroup("ad", "X"))),
        )

      val result = filterProcessor.processBatch(batch)

      assertThat(result.events.map { it.vid }).containsExactly(1L)
    }
  }

  @Test
  fun `processBatch with entityKeys filter combined with CEL must satisfy both`() {
    runBlocking {
      val targetKey = makeEntityKey("ad", "X")
      val filterSpec =
        createTestFilterSpec(
          celExpression = "person.age_group == 1", // YEARS_18_TO_34
          entityKeys = setOf(targetKey),
        )
      val filterProcessor = FilterProcessor<Message>(filterSpec, testEventDescriptor)

      val events =
        listOf(
          // matches both
          createTestLabeledEvent(
            1,
            ageGroup = Person.AgeGroup.YEARS_18_TO_34,
            entityKeys = listOf(targetKey),
          ),
          // matches CEL but not entity key
          createTestLabeledEvent(
            2,
            ageGroup = Person.AgeGroup.YEARS_18_TO_34,
            entityKeys = listOf(makeEntityKey("ad", "Y")),
          ),
          // matches entity key but not CEL
          createTestLabeledEvent(
            3,
            ageGroup = Person.AgeGroup.YEARS_35_TO_54,
            entityKeys = listOf(targetKey),
          ),
        )

      val result =
        filterProcessor.processBatch(
          createEventBatch(events, unmappedEntityKeys(listOf(makeEntityKeyGroup("ad", "X", "Y"))))
        )

      assertThat(result.events.map { it.vid }).containsExactly(1L)
    }
  }

  @Test
  fun `processBatch checks entity keys when reference ID does not match`() {
    runBlocking {
      // A nonmatching representation does not prevent the other representation from matching.
      val targetKey = makeEntityKey("ad", "X")
      val filterSpec =
        createTestFilterSpec(
          eventGroupReferenceIds = setOf("some-other-group"),
          entityKeys = setOf(targetKey),
        )
      val filterProcessor = FilterProcessor<Message>(filterSpec, testEventDescriptor)

      val events =
        listOf(
          createTestLabeledEvent(vid = 1, entityKeys = listOf(targetKey)),
          createTestLabeledEvent(vid = 2, entityKeys = listOf(makeEntityKey("ad", "Y"))),
        )

      val result =
        filterProcessor.processBatch(
          createEventBatch(
            events,
            EventGroupIdentifier(
              eventGroupReferenceId = "test-group",
              entityKeys = setOf(targetKey, makeEntityKey("ad", "Y")),
              wholeBlobEntityIdsByType = emptyMap(),
            ),
          )
        )

      // The reference ID is absent from the filter, but the entity-key intersection still applies.
      assertThat(result.events.map { it.vid }).containsExactly(1L)
    }
  }

  @Test
  fun `FilterSpec construction throws when all event group selectors are empty`() {
    assertFailsWith<IllegalArgumentException> {
      FilterSpec(
        celExpression = "",
        collectionInterval = createDefaultInterval(),
        eventGroupReferenceIds = emptySet(),
        entityKeys = emptySet(),
      )
    }
  }

  @Test
  fun `FilterSpec construction accepts entity key selector`() {
    // No exception expected.
    FilterSpec(
      celExpression = "",
      collectionInterval = createDefaultInterval(),
      entityKeys = setOf(makeEntityKey("ad", "X")),
      eventGroupReferenceIds = emptySet(),
    )
  }

  @Test
  fun `FilterSpec construction accepts event group reference ID selector`() {
    // No exception expected.
    FilterSpec(
      celExpression = "",
      collectionInterval = createDefaultInterval(),
      eventGroupReferenceIds = setOf("test-group"),
      entityKeys = emptySet(),
    )
  }

  @Test
  fun `FilterSpec construction throws for invalid interval with reference ID selector`() {
    assertFailsWith<InvalidCollectionIntervalException> {
      FilterSpec(
        celExpression = "",
        collectionInterval = createInvalidInterval(),
        eventGroupReferenceIds = setOf("test-group"),
        entityKeys = emptySet(),
      )
    }
  }

  @Test
  fun `FilterSpec construction throws for invalid interval with entity key selector`() {
    assertFailsWith<InvalidCollectionIntervalException> {
      FilterSpec(
        celExpression = "",
        collectionInterval = createInvalidInterval(),
        entityKeys = setOf(makeEntityKey("ad", "X")),
        eventGroupReferenceIds = emptySet(),
      )
    }
  }

  @Test
  fun `processBatch with batch entityKeys overlapping the filter passes events through`() {
    runBlocking {
      val targetKey = makeEntityKey("ad", "X")
      val filterSpec = createTestFilterSpec(entityKeys = setOf(targetKey))
      val filterProcessor = FilterProcessor<Message>(filterSpec, testEventDescriptor)

      // Batch carries an EntityKeyGroup that overlaps the filter — batch-level check passes.
      // Per-event check then keeps only events whose own entity_keys intersect the filter.
      val events =
        listOf(
          createTestLabeledEvent(1, entityKeys = listOf(targetKey)),
          createTestLabeledEvent(2, entityKeys = listOf(makeEntityKey("ad", "Y"))),
        )
      val batchKeys = listOf(makeEntityKeyGroup("ad", "X", "Y"))

      val result =
        filterProcessor.processBatch(createEventBatch(events, unmappedEntityKeys(batchKeys)))

      assertThat(result.events.map { it.vid }).containsExactly(1L)
    }
  }

  @Test
  fun `processBatch with batch entityKeys not overlapping the filter returns empty batch`() {
    runBlocking {
      val filterSpec = createTestFilterSpec(entityKeys = setOf(makeEntityKey("ad", "X")))
      val filterProcessor = FilterProcessor<Message>(filterSpec, testEventDescriptor)

      // Events would match per-event but the batch's own entity_keys do not overlap the
      // filter — the entire batch is dropped before the per-event check runs.
      val events = listOf(createTestLabeledEvent(1, entityKeys = listOf(makeEntityKey("ad", "X"))))
      val batchKeys = listOf(makeEntityKeyGroup("placement", "P"))

      val result =
        filterProcessor.processBatch(createEventBatch(events, unmappedEntityKeys(batchKeys)))

      assertThat(result.events).isEmpty()
    }
  }

  @Test
  fun `processBatch keeps all events when configured singleton key matches filter`() {
    runBlocking {
      val filterSpec =
        createTestFilterSpec(entityKeys = setOf(makeEntityKey("ad", "X"), makeEntityKey("ad", "Y")))
      val filterProcessor = FilterProcessor<Message>(filterSpec, testEventDescriptor)

      val events =
        listOf(
          createTestLabeledEvent(1, entityKeys = listOf(makeEntityKey("ad", "X"))),
          createTestLabeledEvent(2, entityKeys = listOf(makeEntityKey("ad", "X"))),
        )

      val result =
        filterProcessor.processBatch(
          createEventBatch(
            events,
            entityKeyIdentifier(
              listOf(makeEntityKeyGroup("ad", "X")),
              wholeBlobEntityTypes = setOf("ad"),
            ),
          )
        )

      assertThat(result.events.map { it.vid }).containsExactly(1L, 2L).inOrder()
    }
  }

  @Test
  fun `processBatch keeps all events when configured singleton type matches filter`() {
    runBlocking {
      val filterSpec =
        createTestFilterSpec(
          entityKeys =
            setOf(
              makeEntityKey("ad", "X"),
              makeEntityKey("ad", "Y"),
              makeEntityKey("placement", "P"),
            )
        )
      val filterProcessor = FilterProcessor<Message>(filterSpec, testEventDescriptor)

      val events =
        listOf(
          createTestLabeledEvent(1, entityKeys = listOf(makeEntityKey("ad", "X"))),
          createTestLabeledEvent(2, entityKeys = listOf(makeEntityKey("ad", "Y"))),
          createTestLabeledEvent(3, entityKeys = listOf(makeEntityKey("placement", "P"))),
        )

      val result =
        filterProcessor.processBatch(
          createEventBatch(
            events,
            entityKeyIdentifier(
              listOf(makeEntityKeyGroup("ad", "X", "Y"), makeEntityKeyGroup("placement", "P")),
              wholeBlobEntityTypes = setOf("placement"),
            ),
          )
        )

      assertThat(result.events.map { it.vid }).containsExactly(1L, 2L, 3L).inOrder()
    }
  }

  @Test
  fun `processBatch keeps all events when all match entity filter`() {
    runBlocking {
      val filterSpec =
        createTestFilterSpec(entityKeys = setOf(makeEntityKey("ad", "X"), makeEntityKey("ad", "Y")))
      val filterProcessor = FilterProcessor<Message>(filterSpec, testEventDescriptor)

      val events =
        listOf(
          createTestLabeledEvent(1, entityKeys = listOf(makeEntityKey("ad", "X"))),
          createTestLabeledEvent(2, entityKeys = listOf(makeEntityKey("ad", "Y"))),
          createTestLabeledEvent(
            3,
            entityKeys = listOf(makeEntityKey("ad", "X"), makeEntityKey("ad", "Y")),
          ),
        )

      val result =
        filterProcessor.processBatch(
          createEventBatch(events, unmappedEntityKeys(listOf(makeEntityKeyGroup("ad", "X", "Y"))))
        )

      assertThat(result.events.map { it.vid }).containsExactly(1L, 2L, 3L).inOrder()
    }
  }

  @Test
  fun `matchBatch returns NoMatch for disjoint entity key sets`() {
    val filterSpec =
      FilterSpec(
        celExpression = "",
        collectionInterval = createDefaultInterval(),
        entityKeys = setOf(makeEntityKey("ad", "X")),
        eventGroupReferenceIds = emptySet(),
      )

    val result =
      filterSpec.matchBatch(unmappedEntityKeys(listOf(makeEntityKeyGroup("placement", "P"))))

    assertThat(result).isEqualTo(BatchMatchResult.NoMatch)
  }

  @Test
  fun `matchBatch returns MatchedAllEvents when configured singleton type matches filter`() {
    val filterSpec =
      FilterSpec(
        celExpression = "",
        collectionInterval = createDefaultInterval(),
        entityKeys = setOf(makeEntityKey("ad", "X"), makeEntityKey("ad", "Y")),
        eventGroupReferenceIds = emptySet(),
      )

    val result =
      filterSpec.matchBatch(
        entityKeyIdentifier(
          listOf(makeEntityKeyGroup("ad", "X")),
          wholeBlobEntityTypes = setOf("ad"),
        )
      )

    assertThat(result).isEqualTo(BatchMatchResult.MatchedAllEvents)
  }

  @Test
  fun `matchBatch returns MatchedByEntityKeys when singleton type is not configured`() {
    val filterSpec =
      FilterSpec(
        celExpression = "",
        collectionInterval = createDefaultInterval(),
        entityKeys = setOf(makeEntityKey("ad", "X")),
        eventGroupReferenceIds = emptySet(),
      )

    val result = filterSpec.matchBatch(unmappedEntityKeys(listOf(makeEntityKeyGroup("ad", "X"))))

    assertThat(result)
      .isEqualTo(
        BatchMatchResult.MatchedByEntityKeys(entityKeyFilter = setOf(makeEntityKey("ad", "X")))
      )
  }

  @Test
  fun `matchBatch returns MatchedByEntityKeys when all values of multi-value type match filter`() {
    val filterSpec =
      FilterSpec(
        celExpression = "",
        collectionInterval = createDefaultInterval(),
        entityKeys = setOf(makeEntityKey("ad", "X"), makeEntityKey("ad", "Y")),
        eventGroupReferenceIds = emptySet(),
      )

    val result =
      filterSpec.matchBatch(unmappedEntityKeys(listOf(makeEntityKeyGroup("ad", "X", "Y"))))

    assertThat(result)
      .isEqualTo(
        BatchMatchResult.MatchedByEntityKeys(
          entityKeyFilter = setOf(makeEntityKey("ad", "X"), makeEntityKey("ad", "Y"))
        )
      )
  }

  @Test
  fun `matchBatch returns MatchedAllEvents when all exhaustive type values match filter`() {
    val filterSpec =
      FilterSpec(
        celExpression = "",
        collectionInterval = createDefaultInterval(),
        entityKeys = setOf(makeEntityKey("campaign", "X"), makeEntityKey("campaign", "Y")),
        eventGroupReferenceIds = emptySet(),
      )

    val result =
      filterSpec.matchBatch(
        entityKeyIdentifier(
          listOf(makeEntityKeyGroup("campaign", "X", "Y")),
          wholeBlobEntityTypes = setOf("campaign"),
        )
      )

    assertThat(result).isEqualTo(BatchMatchResult.MatchedAllEvents)
  }

  @Test
  fun `matchBatch filters events when only some exhaustive type values match filter`() {
    val filterSpec =
      FilterSpec(
        celExpression = "",
        collectionInterval = createDefaultInterval(),
        entityKeys = setOf(makeEntityKey("campaign", "X")),
        eventGroupReferenceIds = emptySet(),
      )

    val result =
      filterSpec.matchBatch(
        entityKeyIdentifier(
          listOf(makeEntityKeyGroup("campaign", "X", "Y")),
          wholeBlobEntityTypes = setOf("campaign"),
        )
      )

    assertThat(result)
      .isEqualTo(
        BatchMatchResult.MatchedByEntityKeys(
          entityKeyFilter = setOf(makeEntityKey("campaign", "X"))
        )
      )
  }

  @Test
  fun `matchBatch returns MatchedByEntityKeys when batch keys partially overlap filter keys`() {
    val filterSpec =
      FilterSpec(
        celExpression = "",
        collectionInterval = createDefaultInterval(),
        entityKeys = setOf(makeEntityKey("ad", "X")),
        eventGroupReferenceIds = emptySet(),
      )

    val result =
      filterSpec.matchBatch(unmappedEntityKeys(listOf(makeEntityKeyGroup("ad", "X", "Y"))))

    assertThat(result).isInstanceOf(BatchMatchResult.MatchedByEntityKeys::class.java)
  }

  @Test
  fun `processBatch accepts mapped reference ID for an entity-key filter`() {
    val filterSpec =
      createTestFilterSpec(entityKeys = setOf(makeEntityKey("campaign", "entity/123")))
    val filterProcessor = FilterProcessor<Message>(filterSpec, testEventDescriptor)
    val events =
      listOf(
        createTestLabeledEvent(1, entityKeys = emptyList()),
        createTestLabeledEvent(2, entityKeys = emptyList()),
      )

    val result =
      filterProcessor.processBatch(
        createEventBatch(
          events,
          EventGroupIdentifier(
            eventGroupReferenceId = "ref/123",
            entityKeys = setOf(makeEntityKey("campaign", "entity/123")),
            wholeBlobEntityIdsByType = mapOf("campaign" to setOf("entity/123")),
          ),
        )
      )

    assertThat(result.events.map { it.vid }).containsExactly(1L, 2L).inOrder()
  }

  @Test
  fun `processBatch accepts all events when mapped blob has one requested campaign`() {
    val filterSpec = createTestFilterSpec(eventGroupReferenceIds = setOf("ref/123"))
    val filterProcessor = FilterProcessor<Message>(filterSpec, testEventDescriptor)
    val events =
      listOf(
        createTestLabeledEvent(1, entityKeys = listOf(makeEntityKey("ad", "A"))),
        createTestLabeledEvent(2, entityKeys = listOf(makeEntityKey("ad", "B"))),
      )

    val result =
      filterProcessor.processBatch(
        createEventBatch(
          events,
          EventGroupIdentifier(
            eventGroupReferenceId = "ref/123",
            entityKeys =
              setOf(
                makeEntityKey("campaign", "entity/123"),
                makeEntityKey("ad", "A"),
                makeEntityKey("ad", "B"),
              ),
            wholeBlobEntityIdsByType = mapOf("campaign" to setOf("entity/123")),
          ),
        )
      )

    assertThat(result.events.map { it.vid }).containsExactly(1L, 2L).inOrder()
  }

  @Test
  fun `processBatch entity filter accepts all events when mapped blob has one requested campaign`() {
    val filterSpec =
      FilterSpec(
        celExpression = "",
        collectionInterval = createDefaultInterval(),
        entityKeys = setOf(makeEntityKey("campaign", "entity/123")),
        eventGroupReferenceIds = setOf("ref/123"),
      )
    val filterProcessor = FilterProcessor<Message>(filterSpec, testEventDescriptor)
    val events =
      listOf(
        createTestLabeledEvent(1, entityKeys = listOf(makeEntityKey("ad", "A"))),
        createTestLabeledEvent(2, entityKeys = listOf(makeEntityKey("ad", "B"))),
      )

    val result =
      filterProcessor.processBatch(
        createEventBatch(
          events,
          EventGroupIdentifier(
            eventGroupReferenceId = "ref/123",
            entityKeys =
              setOf(
                makeEntityKey("campaign", "entity/123"),
                makeEntityKey("ad", "A"),
                makeEntityKey("ad", "B"),
              ),
            wholeBlobEntityIdsByType = mapOf("campaign" to setOf("entity/123")),
          ),
        )
      )

    assertThat(result.events.map { it.vid }).containsExactly(1L, 2L).inOrder()
  }

  @Test
  fun `processBatch filters events when mapped blob has multiple campaigns`() {
    val filterSpec =
      FilterSpec(
        celExpression = "",
        collectionInterval = createDefaultInterval(),
        eventGroupReferenceIds = setOf("ref/123"),
        entityKeys = setOf(makeEntityKey("campaign", "entity/123")),
      )
    val filterProcessor = FilterProcessor<Message>(filterSpec, testEventDescriptor)
    val matchingCampaign = makeEntityKey("campaign", "entity/123")
    val events =
      listOf(
        createTestLabeledEvent(1, entityKeys = listOf(matchingCampaign)),
        createTestLabeledEvent(2, entityKeys = listOf(makeEntityKey("campaign", "entity/456"))),
      )

    val result =
      filterProcessor.processBatch(
        createEventBatch(
          events,
          EventGroupIdentifier(
            eventGroupReferenceId = null,
            entityKeys =
              setOf(
                makeEntityKey("campaign", "entity/123"),
                makeEntityKey("campaign", "entity/456"),
              ),
            wholeBlobEntityIdsByType = emptyMap(),
          ),
        )
      )

    assertThat(result.events.map { it.vid }).containsExactly(1L)
  }

  @Test
  fun `processBatch accepts direct reference ID when mapping is configured`() {
    val filterSpec = createTestFilterSpec(eventGroupReferenceIds = setOf("direct-reference-id"))
    val filterProcessor = FilterProcessor<Message>(filterSpec, testEventDescriptor)
    val events = listOf(createTestLabeledEvent(1, entityKeys = emptyList()))

    val result =
      filterProcessor.processBatch(
        createEventBatch(
          events,
          EventGroupIdentifier(
            eventGroupReferenceId = "direct-reference-id",
            entityKeys = emptySet(),
            wholeBlobEntityIdsByType = emptyMap(),
          ),
        )
      )

    assertThat(result.events.map { it.vid }).containsExactly(1L)
  }
}
