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
import com.google.protobuf.Timestamp
import com.google.protobuf.TypeRegistry
import com.google.type.interval
import java.time.LocalDate
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters

@RunWith(JUnit4::class)
class VidFilterTest {

  @Test
  fun `filter returns impressions when all conditions are met`() = runBlocking {
    // Set up test environment
    val testEventDescriptor = TestEvent.getDescriptor()

    // Create TypeRegistry with the test event descriptor
    val typeRegistry = TypeRegistry.newBuilder()
      .add(testEventDescriptor)
      .build()

    // Create collection interval
    val collectionInterval = interval {
      startTime = TIME_RANGE.start.toProtoTime()
      endTime = TIME_RANGE.endExclusive.toProtoTime()
    }

    // Create event filter
    val filter = eventFilter { expression = "person.gender == 1" } // MALE is 1

    // Create a program for the filter
    val program = EventFilters.compileProgram(testEventDescriptor, filter.expression)

    // Create VidFilter
    val vidFilter = VidFilter(
      program,
      collectionInterval,
      0.0f,
      1.0f,
      typeRegistry
    )

    // Create labeled impression with event time within collection interval
    val labeledImpression = LABELED_IMPRESSION_1.copy {
      eventTime = TIME_RANGE.start.toProtoTime()
    }

    // Call the filter method
    val result = vidFilter.filter(listOf(labeledImpression).asFlow()).toList()

    // Verify the result
    assertThat(result).hasSize(1)
    assertThat(result[0]).isEqualTo(labeledImpression)
  }

  @Test
  fun `filter excludes impressions when event time is outside collection interval`() = runBlocking {
    // Set up test environment
    val testEventDescriptor = TestEvent.getDescriptor()

    // Create TypeRegistry with the test event descriptor
    val typeRegistry = TypeRegistry.newBuilder()
      .add(testEventDescriptor)
      .build()

    // Create collection interval
    val collectionInterval = interval {
      startTime = TIME_RANGE.start.toProtoTime()
      endTime = TIME_RANGE.endExclusive.toProtoTime()
    }

    // Create event filter
    val filter = eventFilter { expression = "person.gender == 1" } // MALE is 1

    // Create a program for the filter
    val program = EventFilters.compileProgram(testEventDescriptor, filter.expression)

    // Create VidFilter
    val vidFilter = VidFilter(
      program,
      collectionInterval,
      0.0f,
      1.0f,
      typeRegistry
    )

    // Create labeled impression with event time BEFORE collection interval
    val beforeIntervalTime = TIME_RANGE.start.minusSeconds(86400).toProtoTime() // 1 day prior
    val labeledImpression = LABELED_IMPRESSION_1.copy {
      eventTime = beforeIntervalTime
    }

    // Call the filter method
    val result = vidFilter.filter(listOf(labeledImpression).asFlow()).toList()

    // Verify the result
    assertThat(result).isEmpty()
  }

  @Test
  fun `filter excludes impressions when event does not match filter`() = runBlocking {
    // Set up test environment
    val testEventDescriptor = TestEvent.getDescriptor()

    // Create TypeRegistry with the test event descriptor
    val typeRegistry = TypeRegistry.newBuilder()
      .add(testEventDescriptor)
      .build()

    // Create collection interval
    val collectionInterval = interval {
      startTime = TIME_RANGE.start.toProtoTime()
      endTime = TIME_RANGE.endExclusive.toProtoTime()
    }

    // Create event filter that requires FEMALE (gender == 2)
    val filter = eventFilter { expression = "person.gender == 2" } // FEMALE is 2

    // Create a program for the filter
    val program = EventFilters.compileProgram(testEventDescriptor, filter.expression)

    // Create VidFilter
    val vidFilter = VidFilter(
      program,
      collectionInterval,
      0.0f,
      1.0f,
      typeRegistry
    )

    // Create labeled impression with MALE gender (1)
    val labeledImpression = LABELED_IMPRESSION_1.copy {
      eventTime = TIME_RANGE.start.toProtoTime()
    }

    // Call the filter method
    val result = vidFilter.filter(listOf(labeledImpression).asFlow()).toList()

    // Verify the result
    assertThat(result).isEmpty()
  }

  @Test
  fun `filter excludes impressions when VID is outside sampling interval`() = runBlocking {
    // Set up test environment
    val testEventDescriptor = TestEvent.getDescriptor()

    // Create TypeRegistry with the test event descriptor
    val typeRegistry = TypeRegistry.newBuilder()
      .add(testEventDescriptor)
      .build()

    // Create collection interval
    val collectionInterval = interval {
      startTime = TIME_RANGE.start.toProtoTime()
      endTime = TIME_RANGE.endExclusive.toProtoTime()
    }

    // Create event filter
    val filter = eventFilter { expression = "person.gender == 1" } // MALE is 1

    // Create a program for the filter
    val program = EventFilters.compileProgram(testEventDescriptor, filter.expression)

    // Create VidFilter with a very narrow sampling interval that should exclude the VID
    val vidFilter = VidFilter(
      program,
      collectionInterval,
      0.9f,  // Very high start
      0.1f,  // Very narrow width
      typeRegistry
    )

    // Create labeled impression
    val labeledImpression = LABELED_IMPRESSION_1.copy {
      eventTime = TIME_RANGE.start.toProtoTime()
    }

    // Call the filter method
    val result = vidFilter.filter(listOf(labeledImpression).asFlow()).toList()

    // Verify the result
    assertThat(result).isEmpty()
  }

  @Test
  fun `filterAndExtractVids returns VIDs when all conditions are met`() = runBlocking {
    // Set up test environment
    val testEventDescriptor = TestEvent.getDescriptor()

    // Create TypeRegistry with the test event descriptor
    val typeRegistry = TypeRegistry.newBuilder()
      .add(testEventDescriptor)
      .build()

    // Create collection interval
    val collectionInterval = interval {
      startTime = TIME_RANGE.start.toProtoTime()
      endTime = TIME_RANGE.endExclusive.toProtoTime()
    }

    // Create event filter
    val filter = eventFilter { expression = "person.gender == 1" } // MALE is 1

    // Create a program for the filter
    val program = EventFilters.compileProgram(testEventDescriptor, filter.expression)

    // Create VidFilter
    val vidFilter = VidFilter(
      program,
      collectionInterval,
      0.0f,
      1.0f,
      typeRegistry
    )

    // Create labeled impression with event time within collection interval
    val labeledImpression = LABELED_IMPRESSION_1.copy {
      eventTime = TIME_RANGE.start.toProtoTime()
      vid = 42L
    }

    // Call the filterAndExtractVids method
    val result = vidFilter.filterAndExtractVids(listOf(labeledImpression).asFlow()).toList()

    // Verify the result
    assertThat(result).hasSize(1)
    assertThat(result[0]).isEqualTo(42L)
  }

  @Test
  fun `filterAndExtractVids excludes VIDs when conditions are not met`() = runBlocking {
    // Set up test environment
    val testEventDescriptor = TestEvent.getDescriptor()

    // Create TypeRegistry with the test event descriptor
    val typeRegistry = TypeRegistry.newBuilder()
      .add(testEventDescriptor)
      .build()

    // Create collection interval
    val collectionInterval = interval {
      startTime = TIME_RANGE.start.toProtoTime()
      endTime = TIME_RANGE.endExclusive.toProtoTime()
    }

    // Create event filter that requires FEMALE (gender == 2)
    val filter = eventFilter { expression = "person.gender == 2" } // FEMALE is 2

    // Create a program for the filter
    val program = EventFilters.compileProgram(testEventDescriptor, filter.expression)

    // Create VidFilter
    val vidFilter = VidFilter(
      program,
      collectionInterval,
      0.0f,
      1.0f,
      typeRegistry
    )

    // Create labeled impression with MALE gender (1)
    val labeledImpression = LABELED_IMPRESSION_1.copy {
      eventTime = TIME_RANGE.start.toProtoTime()
      vid = 42L
    }

    // Call the filterAndExtractVids method
    val result = vidFilter.filterAndExtractVids(listOf(labeledImpression).asFlow()).toList()

    // Verify the result
    assertThat(result).isEmpty()
  }

  companion object {
    private val LAST_EVENT_DATE = LocalDate.now()
    private val FIRST_EVENT_DATE = LAST_EVENT_DATE.minusDays(1)
    private val TIME_RANGE = OpenEndTimeRange.fromClosedDateRange(FIRST_EVENT_DATE..LAST_EVENT_DATE)

    private val PERSON_1 = person {
      ageGroup = Person.AgeGroup.YEARS_18_TO_34
      gender = Person.Gender.MALE
      socialGradeGroup = Person.SocialGradeGroup.A_B_C1
    }

    private val PERSON_2 = person {
      ageGroup = Person.AgeGroup.YEARS_18_TO_34
      gender = Person.Gender.FEMALE
      socialGradeGroup = Person.SocialGradeGroup.A_B_C1
    }

    private val TEST_EVENT_1 = testEvent { person = PERSON_1 }

    private val TEST_EVENT_2 = testEvent { person = PERSON_2 }

    private val LABELED_IMPRESSION_1 =
      LabeledImpression.newBuilder()
        .setEventTime(Timestamp.getDefaultInstance())
        .setVid(10L)
        .setEvent(
          TEST_EVENT_1.pack()
        )
        .build()

    private val LABELED_IMPRESSION_2 =
      LabeledImpression.newBuilder()
        .setEventTime(Timestamp.getDefaultInstance())
        .setVid(10L)
        .setEvent(
          TEST_EVENT_2.pack()
        )
        .build()
  }
} 
