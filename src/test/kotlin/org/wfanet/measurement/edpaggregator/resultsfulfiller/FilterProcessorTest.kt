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
import com.google.protobuf.util.Timestamps
import com.google.type.Interval
import java.time.Instant
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpressionKt.entityKey

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
  private fun createEventBatch(events: List<LabeledEvent<Message>>): EventBatch<Message> {
    val timestamps = events.map { it.timestamp }
    val minTime = timestamps.minOrNull() ?: Instant.now()
    val maxTime = timestamps.maxOrNull() ?: Instant.now()
    return EventBatch(events, minTime, maxTime, eventGroupReferenceId = "test-group")
  }

  /** Helper function to create a default interval that covers a wide time range. */
  private fun createDefaultInterval(): Interval {
    val startTime = Instant.parse("2000-01-01T00:00:00Z")
    val endTime = Instant.parse("2050-12-31T23:59:59Z")

    return Interval.newBuilder()
      .setStartTime(Timestamps.fromMillis(startTime.toEpochMilli()))
      .setEndTime(Timestamps.fromMillis(endTime.toEpochMilli()))
      .build()
  }

  /** Helper function to create a FilterSpec for testing. */
  private fun createTestFilterSpec(
    celExpression: String = "",
    collectionInterval: Interval = createDefaultInterval(),
    eventGroupReferenceIds: List<String> = listOf("test-group"),
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
  private fun makeEntityKey(type: String, id: String): LabeledImpression.EntityKey =
    entityKey {
      entityType = type
      entityId = id
    }

  @Test
  fun `processBatch filters events with CEL expression`() {
    runBlocking {
      val celExpression = "person.age_group == 1" // YEARS_18_TO_34
      val filterSpec = createTestFilterSpec(celExpression = celExpression)
      val filterProcessor = FilterProcessor<Message>(filterSpec, testEventDescriptor)

      val events =
        listOf(
          createTestLabeledEvent(vid = 1, ageGroup = Person.AgeGroup.YEARS_18_TO_34, entityKeys = emptyList()),
          createTestLabeledEvent(vid = 2, ageGroup = Person.AgeGroup.YEARS_35_TO_54, entityKeys = emptyList()),
          createTestLabeledEvent(vid = 3, ageGroup = Person.AgeGroup.YEARS_18_TO_34, entityKeys = emptyList()),
        )

      val batch = createEventBatch(events)
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
          createTestLabeledEvent(1, ageGroup = Person.AgeGroup.YEARS_18_TO_34, entityKeys = emptyList()),
          createTestLabeledEvent(2, ageGroup = Person.AgeGroup.YEARS_35_TO_54, entityKeys = emptyList()),
          createTestLabeledEvent(3, ageGroup = Person.AgeGroup.YEARS_55_PLUS, entityKeys = emptyList()),
        )

      val batch = createEventBatch(events)
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

      val batch = createEventBatch(events)
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
        createTestFilterSpec(
          celExpression = "",
          eventGroupReferenceIds = listOf(targetEventGroupId),
        )
      val filterProcessor = FilterProcessor<Message>(filterSpec, testEventDescriptor)

      val events =
        listOf(createTestLabeledEvent(1, entityKeys = emptyList()), createTestLabeledEvent(2, entityKeys = emptyList()), createTestLabeledEvent(3, entityKeys = emptyList()))

      val batch = createEventBatch(events)
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
          eventGroupReferenceIds = listOf(targetEventGroupId),
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

      val batch = createEventBatch(events)
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

      val batch = createEventBatch(events)
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
          createTestLabeledEvent(1, timestamp = Instant.parse("2024-12-01T00:00:00Z"), entityKeys = emptyList()),
          createTestLabeledEvent(2, timestamp = Instant.parse("2024-12-15T12:00:00Z"), entityKeys = emptyList()),
          createTestLabeledEvent(3, timestamp = Instant.parse("2024-12-31T23:59:59Z"), entityKeys = emptyList()),
        )

      val batch = createEventBatch(events)
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

      val batch = createEventBatch(events)
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
          createTestLabeledEvent(1, timestamp = Instant.parse("2025-02-01T00:00:00Z"), entityKeys = emptyList()),
          createTestLabeledEvent(2, timestamp = Instant.parse("2025-03-15T12:00:00Z"), entityKeys = emptyList()),
          createTestLabeledEvent(3, timestamp = Instant.parse("2025-04-01T00:00:00Z"), entityKeys = emptyList()),
        )

      val batch = createEventBatch(events)
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
          createTestLabeledEvent(2, timestamp = startTime, entityKeys = emptyList()), // Exactly at start (inclusive)
          createTestLabeledEvent(
            3,
            timestamp = Instant.parse("2025-01-01T12:00:00Z"),
            entityKeys = emptyList(),
          ), // Within range
          createTestLabeledEvent(4, timestamp = endTime.minusMillis(1), entityKeys = emptyList()), // Just before end
          createTestLabeledEvent(5, timestamp = endTime, entityKeys = emptyList()), // Exactly at end (exclusive)
        )

      val batch = createEventBatch(events)
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

      val result = filterProcessor.processBatch(createEventBatch(events))

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
          createTestLabeledEvent(
            2,
            entityKeys = listOf(makeEntityKey("ad", "Y"), targetKey),
          ),
        )

      val result = filterProcessor.processBatch(createEventBatch(events))

      assertThat(result.events.map { it.vid }).containsExactly(1L, 2L).inOrder()
    }
  }

  @Test
  fun `processBatch with entityKeys filter drops events whose keys do not intersect`() {
    runBlocking {
      val filterSpec =
        createTestFilterSpec(entityKeys = setOf(makeEntityKey("ad", "X")))
      val filterProcessor = FilterProcessor<Message>(filterSpec, testEventDescriptor)

      val events =
        listOf(
          createTestLabeledEvent(1, entityKeys = listOf(makeEntityKey("ad", "Y"))),
          createTestLabeledEvent(
            2,
            entityKeys = listOf(makeEntityKey("placement", "X")),
          ),
        )

      val result = filterProcessor.processBatch(createEventBatch(events))

      assertThat(result.events).isEmpty()
    }
  }

  @Test
  fun `processBatch with entityKeys filter drops events whose entityKeys are empty`() {
    runBlocking {
      val filterSpec =
        createTestFilterSpec(entityKeys = setOf(makeEntityKey("ad", "X")))
      val filterProcessor = FilterProcessor<Message>(filterSpec, testEventDescriptor)

      val events = listOf(createTestLabeledEvent(1, entityKeys = emptyList()))

      val result = filterProcessor.processBatch(createEventBatch(events))

      assertThat(result.events).isEmpty()
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

      val result = filterProcessor.processBatch(createEventBatch(events))

      assertThat(result.events.map { it.vid }).containsExactly(1L)
    }
  }
  @Test
  fun `processBatch with entityKeys ignores event_group_reference_id batch check`() {
    runBlocking {
      // When entity_keys is set, it replaces event_group_reference_ids as the selector. A batch
      // whose event_group_reference_id is NOT in the spec should still be processed (and filtered
      // by entity_keys per-event) instead of being dropped wholesale.
      val targetKey = makeEntityKey("ad", "X")
      val filterSpec =
        createTestFilterSpec(
          eventGroupReferenceIds = listOf("some-other-group"),
          entityKeys = setOf(targetKey),
        )
      val filterProcessor = FilterProcessor<Message>(filterSpec, testEventDescriptor)

      val events =
        listOf(
          createTestLabeledEvent(vid = 1, entityKeys = listOf(targetKey)),
          createTestLabeledEvent(vid = 2, entityKeys = listOf(makeEntityKey("ad", "Y"))),
        )

      val result = filterProcessor.processBatch(createEventBatch(events))

      // Batch's event_group_reference_id "test-group" is NOT in the spec, but it is processed
      // anyway because entityKeys takes precedence.
      assertThat(result.events.map { it.vid }).containsExactly(1L)
    }
  }

  @Test
  fun `FilterSpec init throws when both eventGroupReferenceIds and entityKeys are empty`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        FilterSpec(
          celExpression = "",
          collectionInterval = createDefaultInterval(),
          eventGroupReferenceIds = emptyList(),
          entityKeys = emptySet(),
        )
      }
    assertThat(exception).hasMessageThat().contains("Either eventGroupReferenceIds or entityKeys")
  }

  @Test
  fun `FilterSpec init accepts entityKeys-only spec`() {
    // No exception expected.
    FilterSpec(
      celExpression = "",
      collectionInterval = createDefaultInterval(),
      eventGroupReferenceIds = emptyList(),
      entityKeys = setOf(makeEntityKey("ad", "X")),
    )
  }

}
