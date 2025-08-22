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
import com.google.protobuf.util.Timestamps
import com.google.type.Interval
import java.time.Instant
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent


@RunWith(JUnit4::class)
class FilterProcessorTest {

  private val testEventDescriptor: Descriptors.Descriptor = TestEvent.getDescriptor()

  /**
   * Helper function to create a DynamicMessage for a TestEvent.
   */
  private fun createDynamicMessage(
    ageGroup: Person.AgeGroup = Person.AgeGroup.YEARS_18_TO_34,
    gender: Person.Gender = Person.Gender.MALE
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
          .build()
      )
      .build()
  }

  /**
   * Helper function to create a LabeledEvent for testing.
   */
  private fun createTestLabeledEvent(
    vid: Long,
    timestamp: Instant = Instant.now(),
    eventGroupReferenceId: String? = null,
    ageGroup: Person.AgeGroup = Person.AgeGroup.YEARS_18_TO_34,
    gender: Person.Gender = Person.Gender.MALE
  ): LabeledEvent<DynamicMessage> {
    val message = createDynamicMessage(ageGroup, gender)
    return LabeledEvent(
      timestamp = timestamp,
      vid = vid,
      message = message,
      eventGroupReferenceId = eventGroupReferenceId ?: "test-group"
    )
  }

  /**
   * Helper function to create a default interval that covers a wide time range.
   */
  private fun createDefaultInterval(): Interval {
    val startTime = Instant.parse("2000-01-01T00:00:00Z")
    val endTime = Instant.parse("2050-12-31T23:59:59Z")

    return Interval.newBuilder()
      .setStartTime(Timestamps.fromMillis(startTime.toEpochMilli()))
      .setEndTime(Timestamps.fromMillis(endTime.toEpochMilli()))
      .build()
  }

  /**
   * Helper function to create a FilterSpec for testing.
   */
  private fun createTestFilterSpec(
    celExpression: String = "",
    collectionInterval: Interval = createDefaultInterval(),
    vidSamplingStart: Int = 0,
    vidSamplingWidth: Int = Int.MAX_VALUE,
    eventGroupReferenceId: String = "test-group"
  ): FilterSpec {
    return FilterSpec(
      celExpression = celExpression,
      collectionInterval = collectionInterval,
      vidSamplingStart = vidSamplingStart,
      vidSamplingWidth = vidSamplingWidth,
      eventGroupReferenceId = eventGroupReferenceId
    )
  }

  @Test
  fun `processBatch filters events with CEL expression`() { runBlocking {
    val celExpression = "person.age_group == 1" // YEARS_18_TO_34
    val filterSpec = createTestFilterSpec(celExpression = celExpression)
    val filterProcessor = FilterProcessor(filterSpec)

    val events = listOf(
      createTestLabeledEvent(1, ageGroup = Person.AgeGroup.YEARS_18_TO_34),
      createTestLabeledEvent(2, ageGroup = Person.AgeGroup.YEARS_35_TO_54),
      createTestLabeledEvent(3, ageGroup = Person.AgeGroup.YEARS_18_TO_34)
    )

    val batch = EventBatch(events, batchId = 1)
    val result = filterProcessor.processBatch(batch)

    assertThat(result.events).hasSize(2)
    assertThat(result.events.map { it.vid }).containsExactly(1L, 3L)
  }}

  @Test
  fun `processBatch with empty CEL expression matches all events`() { runBlocking {
    val filterSpec = createTestFilterSpec(celExpression = "")
    val filterProcessor = FilterProcessor(filterSpec)

    val events = listOf(
      createTestLabeledEvent(1, ageGroup = Person.AgeGroup.YEARS_18_TO_34),
      createTestLabeledEvent(2, ageGroup = Person.AgeGroup.YEARS_35_TO_54),
      createTestLabeledEvent(3, ageGroup = Person.AgeGroup.YEARS_55_PLUS)
    )

    val batch = EventBatch(events, batchId = 1)
    val result = filterProcessor.processBatch(batch)

    assertThat(result.events).hasSize(3)
    assertThat(result.events.map { it.vid }).containsExactly(1L, 2L, 3L)
  }}

  @Test
  fun `processBatch filters by time range`() { runBlocking {
    val startTime = Instant.parse("2025-01-01T00:00:00Z")
    val endTime = Instant.parse("2025-01-31T23:59:59Z")

    val interval = Interval.newBuilder()
      .setStartTime(Timestamps.fromMillis(startTime.toEpochMilli()))
      .setEndTime(Timestamps.fromMillis(endTime.toEpochMilli()))
      .build()

    val filterSpec = createTestFilterSpec(
      celExpression = "",
      collectionInterval = interval
    )
    val filterProcessor = FilterProcessor(filterSpec)

    val events = listOf(
      createTestLabeledEvent(1, timestamp = Instant.parse("2024-12-31T23:59:59Z")), // Before range
      createTestLabeledEvent(2, timestamp = Instant.parse("2025-01-15T12:00:00Z")), // Within range
      createTestLabeledEvent(3, timestamp = Instant.parse("2025-02-01T00:00:00Z"))  // After range
    )

    val batch = EventBatch(events, batchId = 1)
    val result = filterProcessor.processBatch(batch)

    // Only event 2 should match (within time range)
    assertThat(result.events).hasSize(1)
    assertThat(result.events[0].vid).isEqualTo(2L)
  }}

  @Test
  fun `processBatch filters by event group reference ID`() { runBlocking {
    val targetEventGroupId = "event-group-1"

    val filterSpec = createTestFilterSpec(
      celExpression = "",
      eventGroupReferenceId = targetEventGroupId
    )
    val filterProcessor = FilterProcessor(filterSpec)

    val events = listOf(
      createTestLabeledEvent(1, eventGroupReferenceId = "event-group-1"),
      createTestLabeledEvent(2, eventGroupReferenceId = "event-group-2"),
      createTestLabeledEvent(3, eventGroupReferenceId = "event-group-1")
    )

    val batch = EventBatch(events, batchId = 1)
    val result = filterProcessor.processBatch(batch)

    assertThat(result.events).hasSize(2)
    assertThat(result.events.map { it.vid }).containsExactly(1L, 3L)
  }}

  @Test
  fun `processBatch applies combined filters`() { runBlocking {
    val celExpression = "person.gender == 1" // MALE
    val targetEventGroupId = "event-group-1"
    val startTime = Instant.parse("2025-01-01T00:00:00Z")
    val endTime = Instant.parse("2025-01-31T23:59:59Z")

    val interval = Interval.newBuilder()
      .setStartTime(Timestamps.fromMillis(startTime.toEpochMilli()))
      .setEndTime(Timestamps.fromMillis(endTime.toEpochMilli()))
      .build()

    val filterSpec = createTestFilterSpec(
      celExpression = celExpression,
      collectionInterval = interval,
      eventGroupReferenceId = targetEventGroupId
    )
    val filterProcessor = FilterProcessor(filterSpec)

    val events = listOf(
      createTestLabeledEvent(1, gender = Person.Gender.MALE, timestamp = Instant.parse("2025-01-15T12:00:00Z"), eventGroupReferenceId = "event-group-1"),
      createTestLabeledEvent(2, gender = Person.Gender.FEMALE, timestamp = Instant.parse("2025-01-15T12:00:00Z"), eventGroupReferenceId = "event-group-1"),
      createTestLabeledEvent(3, gender = Person.Gender.MALE, timestamp = Instant.parse("2025-01-15T12:00:00Z"), eventGroupReferenceId = "event-group-2"),
      createTestLabeledEvent(4, gender = Person.Gender.MALE, timestamp = Instant.parse("2024-12-31T23:59:59Z"), eventGroupReferenceId = "event-group-1")
    )

    val batch = EventBatch(events, batchId = 1)
    val result = filterProcessor.processBatch(batch)

    // Only event 1 should match all criteria (male, correct group, within time range)
    assertThat(result.events).hasSize(1)
    assertThat(result.events[0].vid).isEqualTo(1L)
  }}

  @Test
  fun `processBatch with complex CEL expression`() { runBlocking {
    val celExpression = "person.age_group == 1 && person.gender == 1" // YEARS_18_TO_34 && MALE
    val filterSpec = createTestFilterSpec(celExpression = celExpression)
    val filterProcessor = FilterProcessor(filterSpec)

    val events = listOf(
      createTestLabeledEvent(1, ageGroup = Person.AgeGroup.YEARS_18_TO_34, gender = Person.Gender.MALE),
      createTestLabeledEvent(2, ageGroup = Person.AgeGroup.YEARS_18_TO_34, gender = Person.Gender.FEMALE),
      createTestLabeledEvent(3, ageGroup = Person.AgeGroup.YEARS_35_TO_54, gender = Person.Gender.MALE),
      createTestLabeledEvent(4, ageGroup = Person.AgeGroup.YEARS_18_TO_34, gender = Person.Gender.MALE)
    )

    val batch = EventBatch(events, batchId = 1)
    val result = filterProcessor.processBatch(batch)

    assertThat(result.events).hasSize(2)
    assertThat(result.events.map { it.vid }).containsExactly(1L, 4L)
  }}

  @Test
  fun `processBatch filters events at time range boundaries`() { runBlocking {
    val startTime = Instant.parse("2025-01-01T00:00:00Z")
    val endTime = Instant.parse("2025-01-02T00:00:00Z")

    val interval = Interval.newBuilder()
      .setStartTime(Timestamps.fromMillis(startTime.toEpochMilli()))
      .setEndTime(Timestamps.fromMillis(endTime.toEpochMilli()))
      .build()

    val filterSpec = createTestFilterSpec(
      celExpression = "",
      collectionInterval = interval
    )
    val filterProcessor = FilterProcessor(filterSpec)

    val events = listOf(
      createTestLabeledEvent(1, timestamp = Instant.parse("2024-12-31T23:59:59.999Z")), // Just before start
      createTestLabeledEvent(2, timestamp = startTime), // Exactly at start (inclusive)
      createTestLabeledEvent(3, timestamp = Instant.parse("2025-01-01T12:00:00Z")), // Within range
      createTestLabeledEvent(4, timestamp = endTime.minusMillis(1)), // Just before end
      createTestLabeledEvent(5, timestamp = endTime) // Exactly at end (exclusive)
    )

    val batch = EventBatch(events, batchId = 1)
    val result = filterProcessor.processBatch(batch)

    // Events 2, 3, and 4 should match (start inclusive, end exclusive)
    assertThat(result.events).hasSize(3)
    assertThat(result.events.map { it.vid }).containsExactly(2L, 3L, 4L)
  }}
}
