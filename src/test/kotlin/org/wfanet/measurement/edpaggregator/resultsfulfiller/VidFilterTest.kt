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

@RunWith(JUnit4::class)
class VidFilterTest {

  @Test
  fun `filterAndExtractVids returns VIDs when all conditions are met`() = runBlocking {
    // Set up test environment
    val testEventDescriptor = TestEvent.getDescriptor()

    // Create TypeRegistry with the test event descriptor
    val typeRegistry = TypeRegistry.newBuilder().add(testEventDescriptor).build()

    // Create collection interval
    val collectionInterval = interval {
      startTime = TIME_RANGE.start.toProtoTime()
      endTime = TIME_RANGE.endExclusive.toProtoTime()
    }

    // Create event filter
    val filter = eventFilter { expression = "person.gender == 1" } // MALE is 1

    val virtualId = 42L

    // Create labeled impression with event time within collection interval
    val labeledImpression =
      LABELED_IMPRESSION_1.copy {
        eventTime = TIME_RANGE.start.toProtoTime()
        vid = virtualId
      }

    // Call the filterAndExtractVids method
    val result =
      VidFilter.filterAndExtractVids(
          listOf(labeledImpression).asFlow(),
          0.0f,
          1.0f,
          filter,
          collectionInterval,
          typeRegistry,
        )
        .toList()

    // Verify the result
    assertThat(result).hasSize(1)
    assertThat(result[0]).isEqualTo(virtualId)
  }

  @Test
  fun `filterAndExtractVids excludes VIDs when conditions are not met`() = runBlocking {
    // Set up test environment
    val testEventDescriptor = TestEvent.getDescriptor()

    // Create TypeRegistry with the test event descriptor
    val typeRegistry = TypeRegistry.newBuilder().add(testEventDescriptor).build()

    // Create collection interval
    val collectionInterval = interval {
      startTime = TIME_RANGE.start.toProtoTime()
      endTime = TIME_RANGE.endExclusive.toProtoTime()
    }

    // Create event filter that requires FEMALE (gender == 2)
    val filter = eventFilter { expression = "person.gender == 2" } // FEMALE is 2

    // Create labeled impression with MALE gender (1)
    val labeledImpression =
      LABELED_IMPRESSION_1.copy {
        eventTime = TIME_RANGE.start.toProtoTime()
        vid = 42L
      }

    // Call the filterAndExtractVids method
    val result =
      VidFilter.filterAndExtractVids(
          listOf(labeledImpression).asFlow(),
          0.0f,
          1.0f,
          filter,
          collectionInterval,
          typeRegistry,
        )
        .toList()

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

    private val TEST_EVENT_1 = testEvent { person = PERSON_1 }

    private val LABELED_IMPRESSION_1 =
      LabeledImpression.newBuilder()
        .setEventTime(Timestamp.getDefaultInstance())
        .setVid(10L)
        .setEvent(TEST_EVENT_1.pack())
        .build()
  }
}
