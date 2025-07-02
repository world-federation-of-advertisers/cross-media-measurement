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
import com.google.protobuf.TypeRegistry
import com.google.protobuf.kotlin.unpack
import com.google.type.interval
import java.time.LocalDate
import java.time.ZoneId
import java.time.ZoneOffset
import kotlin.random.Random
import kotlinx.coroutines.flow.count
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertThrows
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.events
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.labeledImpression

@RunWith(JUnit4::class)
class RequisitionSpecsTest {

  @Test
  fun `getSampledVids filters per cel filter`() = runBlocking {
    // Set up test environment
    val testEventDescriptor = TestEvent.getDescriptor()

    // Create TypeRegistry with the test event descriptor
    val typeRegistry = TypeRegistry.newBuilder().add(testEventDescriptor).build()

    // Create sampling interval
    val vidSamplingInterval = vidSamplingInterval {
      start = 0.0f
      width = 1.0f
    }
    val labeledImpression = labeledImpression {
      vid = 1
      eventTime = FIRST_EVENT_DATE.atTime(1, 1, 1).toInstant(ZoneOffset.UTC).toProtoTime()
      event =
        testEvent {
            this.person = person {
              gender = Person.Gender.FEMALE
              ageGroup = Person.AgeGroup.YEARS_18_TO_34
            }
          }
          .pack()
    }
    val impressions =
      flowOf(
        labeledImpression,
        labeledImpression.copy {
          this.event =
            this.event
              .unpack(TestEvent::class.java)
              .toBuilder()
              .apply {
                this.person = this.person.toBuilder().apply { gender = Person.Gender.MALE }.build()
              }
              .build()
              .pack()
        },
      )
    val eventReader: EventReader =
      mock<EventReader> {
        onBlocking { getLabeledImpressions(any(), any()) }.thenReturn(impressions)
      }

    val result =
      RequisitionSpecs.getSampledVids(
        REQUISITION_SPEC,
        EVENT_GROUP_MAP,
        vidSamplingInterval,
        typeRegistry,
        eventReader,
        ZoneOffset.UTC,
      )

    assertThat(result.count()).isEqualTo(1)
  }

  @Test
  fun `getSampledVids filters by vid interval`() = runBlocking {
    // Set up test environment
    val testEventDescriptor = TestEvent.getDescriptor()

    // Create TypeRegistry with the test event descriptor
    val typeRegistry = TypeRegistry.newBuilder().add(testEventDescriptor).build()

    // Create sampling interval
    val vidSamplingInterval = vidSamplingInterval {
      start = 0.0f
      width = 0.5f
    }
    val labeledImpression = labeledImpression {
      vid = 1
      eventTime = FIRST_EVENT_DATE.atTime(1, 1, 1).toInstant(ZoneOffset.UTC).toProtoTime()
      event =
        testEvent {
            this.person = person {
              gender = Person.Gender.FEMALE
              ageGroup = Person.AgeGroup.YEARS_18_TO_34
            }
          }
          .pack()
    }
    val impressions = flowOf(labeledImpression, labeledImpression.copy { this.vid = 10 })
    val eventReader: EventReader =
      mock<EventReader> {
        onBlocking { getLabeledImpressions(any(), any()) }.thenReturn(impressions)
      }

    val result =
      RequisitionSpecs.getSampledVids(
        REQUISITION_SPEC,
        EVENT_GROUP_MAP,
        vidSamplingInterval,
        typeRegistry,
        eventReader,
        ZoneOffset.UTC,
      )
    assertThat(result.count()).isEqualTo(1)
  }

  @Test
  fun `getSampledVids filters by collection interval for a single day`() = runBlocking {
    // Set up test environment
    val testEventDescriptor = TestEvent.getDescriptor()

    // Create TypeRegistry with the test event descriptor
    val typeRegistry = TypeRegistry.newBuilder().add(testEventDescriptor).build()

    // Create sampling interval
    val vidSamplingInterval = vidSamplingInterval {
      start = 0.0f
      width = 1.0f
    }
    val labeledImpression = labeledImpression {
      vid = 1
      eventTime = FIRST_EVENT_DATE.atTime(1, 1, 1).toInstant(ZoneOffset.UTC).toProtoTime()
      event =
        testEvent {
            this.person = person {
              gender = Person.Gender.FEMALE
              ageGroup = Person.AgeGroup.YEARS_18_TO_34
            }
          }
          .pack()
    }
    val impressions =
      flowOf(
        labeledImpression,
        labeledImpression.copy {
          eventTime =
            FIRST_EVENT_DATE.plusDays(1).atTime(1, 1, 1).toInstant(ZoneOffset.UTC).toProtoTime()
        },
      )
    val eventReader: EventReader =
      mock<EventReader> {
        onBlocking { getLabeledImpressions(any(), any()) }.thenReturn(impressions)
      }

    val result =
      RequisitionSpecs.getSampledVids(
        REQUISITION_SPEC,
        EVENT_GROUP_MAP,
        vidSamplingInterval,
        typeRegistry,
        eventReader,
        ZoneOffset.UTC,
      )

    assertThat(result.count()).isEqualTo(1)
    verifyBlocking(eventReader, times(1)) { getLabeledImpressions(any(), any()) }
  }

  fun `throws exception for invalid vid interval`() = runBlocking {
    // Set up test environment
    val testEventDescriptor = TestEvent.getDescriptor()

    // Create TypeRegistry with the test event descriptor
    val typeRegistry = TypeRegistry.newBuilder().add(testEventDescriptor).build()

    // Create sampling interval
    val vidSamplingInterval = vidSamplingInterval {
      start = 0.5f
      width = 0.6f
    }
    // This event gets filters out since event interval end is at 11PM.
    val labeledImpression = labeledImpression {
      vid = 1
      eventTime =
        FIRST_EVENT_DATE.plusDays(1)
          .atStartOfDay()
          .minusSeconds(1)
          .toInstant(ZoneOffset.UTC)
          .toProtoTime()
      event =
        testEvent {
            this.person = person {
              gender = Person.Gender.FEMALE
              ageGroup = Person.AgeGroup.YEARS_18_TO_34
            }
          }
          .pack()
    }
    val impressions = flowOf(labeledImpression)
    val eventReader: EventReader =
      mock<EventReader> {
        onBlocking { getLabeledImpressions(any(), any()) }.thenReturn(impressions)
      }

    assertThrows(IllegalArgumentException::class.java) {
      runBlocking {
        RequisitionSpecs.getSampledVids(
          REQUISITION_SPEC,
          EVENT_GROUP_MAP,
          vidSamplingInterval,
          typeRegistry,
          eventReader,
          ZoneOffset.UTC,
        )
      }
    }
  }

  companion object {
    private val ZONE_ID = ZoneId.of("UTC")
    private val FIRST_EVENT_DATE = LocalDate.now(ZONE_ID)
    private val LAST_EVENT_DATE = FIRST_EVENT_DATE
    private const val EVENT_GROUP_NAME = "dataProviders/someDataProvider/eventGroups/name"
    private val REQUISITION_SPEC = requisitionSpec {
      events =
        RequisitionSpecKt.events {
          eventGroups +=
            RequisitionSpecKt.eventGroupEntry {
              key = EVENT_GROUP_NAME
              value =
                RequisitionSpecKt.EventGroupEntryKt.value {
                  collectionInterval = interval {
                    startTime =
                      FIRST_EVENT_DATE.atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
                    endTime =
                      LAST_EVENT_DATE.plusDays(1)
                        .atStartOfDay()
                        .minusHours(1)
                        .toInstant(ZoneOffset.UTC)
                        .toProtoTime()
                  }
                  filter =
                    RequisitionSpecKt.eventFilter {
                      expression =
                        "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE} && " +
                          "person.gender == ${Person.Gender.FEMALE_VALUE}"
                    }
                }
            }
        }
      nonce = Random.nextLong()
    }
    private val EVENT_GROUP_MAP: Map<String, String> =
      mapOf(EVENT_GROUP_NAME to "some-event-group-reference-id")
  }
}
