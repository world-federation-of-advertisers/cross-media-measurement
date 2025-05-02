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

package org.wfanet.measurement.edpaggregator.eventgroups

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.timestamp
import com.google.type.interval
import java.time.Clock
import java.time.Duration
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.api.v2alpha.CreateEventGroupRequest
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataKt.AdMetadataKt.campaignMetadata as cmmsCampaignMetadata
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataKt.adMetadata as cmmsAdMetadata
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.MediaType as CmmsMediaType
import org.wfanet.measurement.api.v2alpha.UpdateEventGroupRequest
import org.wfanet.measurement.api.v2alpha.eventGroup as cmmsEventGroup
import org.wfanet.measurement.api.v2alpha.eventGroupMetadata as cmmsEventGroupMetadata
import org.wfanet.measurement.api.v2alpha.listEventGroupsResponse
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup.MediaType
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.AdMetadataKt.campaignMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.adMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.metadata as eventGroupMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.eventGroup

@RunWith(JUnit4::class)
class EventGroupSyncTest {

  private val eventGroupsServiceMock: EventGroupsCoroutineImplBase = mockService {
    onBlocking { updateEventGroup(any<UpdateEventGroupRequest>()) }
      .thenAnswer { invocation -> invocation.getArgument<UpdateEventGroupRequest>(0).eventGroup }
    onBlocking { createEventGroup(any<CreateEventGroupRequest>()) }
      .thenAnswer { invocation -> invocation.getArgument<CreateEventGroupRequest>(0).eventGroup }
    onBlocking { listEventGroups(any<ListEventGroupsRequest>()) }
      .thenAnswer {
        listEventGroupsResponse {
          eventGroups +=
            listOf(
              cmmsEventGroup {
                name = "dataProviders/data-provider-1/eventGroups/reference-id-1"
                measurementConsumer = "measurementConsumers/measurement-consumer-1"
                eventGroupReferenceId = "reference-id-1"
                mediaTypes += listOf("VIDEO", "DISPLAY").map { CmmsMediaType.valueOf(it) }
                eventGroupMetadata = cmmsEventGroupMetadata {
                  this.adMetadata = cmmsAdMetadata {
                    this.campaignMetadata = cmmsCampaignMetadata {
                      brandName = "brand-1"
                      campaignName = "campaign-1"
                    }
                  }
                }
                dataAvailabilityInterval = interval {
                  startTime = timestamp { seconds = 200 }
                  endTime = timestamp { seconds = 300 }
                }
              },
              cmmsEventGroup {
                name = "dataProviders/data-provider-2/eventGroups/reference-id-2"
                measurementConsumer = "measurementConsumers/measurement-consumer-2"
                eventGroupReferenceId = "reference-id-2"
                mediaTypes += listOf("OTHER").map { CmmsMediaType.valueOf(it) }
                eventGroupMetadata = cmmsEventGroupMetadata {
                  this.adMetadata = cmmsAdMetadata {
                    this.campaignMetadata = cmmsCampaignMetadata {
                      brandName = "brand-2"
                      campaignName = "campaign-2"
                    }
                  }
                }
                dataAvailabilityInterval = interval {
                  startTime = timestamp { seconds = 200 }
                  endTime = timestamp { seconds = 300 }
                }
              },
              cmmsEventGroup {
                name = "dataProviders/data-provider-3/eventGroups/reference-id-3"
                measurementConsumer = "measurementConsumers/measurement-consumer-2"
                eventGroupReferenceId = "reference-id-3"
                mediaTypes += listOf(CmmsMediaType.valueOf("OTHER"))
                eventGroupMetadata = cmmsEventGroupMetadata {
                  this.adMetadata = cmmsAdMetadata {
                    this.campaignMetadata = cmmsCampaignMetadata {
                      brandName = "new-brand-name"
                      campaignName = "campaign-3"
                    }
                  }
                }
                dataAvailabilityInterval = interval {
                  startTime = timestamp { seconds = 200 }
                  endTime = timestamp { seconds = 300 }
                }
              },
            )
        }
      }
  }

  private val eventGroupsStub: EventGroupsCoroutineStub by lazy {
    EventGroupsCoroutineStub(grpcTestServerRule.channel)
  }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(eventGroupsServiceMock) }

  @Test
  fun `sync registersUnregisteredEventGroups`() {
    val newCampaign = eventGroup {
      eventGroupReferenceId = "reference-id-4"
      this.eventGroupMetadata = eventGroupMetadata {
        this.adMetadata = adMetadata {
          this.campaignMetadata = campaignMetadata {
            brand = "brand-2"
            campaign = "campaign-2"
          }
        }
      }
      measurementConsumer = "measurement-consumer-2"
      dataAvailabilityInterval = interval {
        startTime = timestamp { seconds = 200 }
        endTime = timestamp { seconds = 300 }
      }
      mediaTypes +=
        listOf(MediaType.valueOf("OTHER"), MediaType.valueOf("VIDEO"), MediaType.valueOf("DISPLAY"))
    }
    val testCampaigns = CAMPAIGNS + newCampaign
    val eventGroupSync =
      EventGroupSync(
        "edp-name",
        eventGroupsStub,
        testCampaigns.asFlow(),
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
      )
    runBlocking { eventGroupSync.sync().collect() }
    verifyBlocking(eventGroupsServiceMock, times(1)) { createEventGroup(any()) }
  }

  @Test
  fun `sync updatesExistingEventGroups`() {
    val eventGroupSync =
      EventGroupSync(
        "edp-name",
        eventGroupsStub,
        CAMPAIGNS.asFlow(),
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
      )
    runBlocking { eventGroupSync.sync().collect() }
    verifyBlocking(eventGroupsServiceMock, times(1)) { updateEventGroup(any()) }
  }

  @Test
  fun `sync returns Map Of Event Group Reference Ids To Event Groups`() {
    runBlocking {
      val eventGroupSync =
        EventGroupSync(
          "edp-name",
          eventGroupsStub,
          CAMPAIGNS.asFlow(),
          MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
        )
      val result = runBlocking { eventGroupSync.sync() }
      assertThat(result.toList().map { it.eventGroupReferenceId to it.eventGroupResource })
        .isEqualTo(
          listOf(
            "reference-id-1" to "dataProviders/data-provider-1/eventGroups/reference-id-1",
            "reference-id-2" to "dataProviders/data-provider-2/eventGroups/reference-id-2",
            "reference-id-3" to "dataProviders/data-provider-3/eventGroups/reference-id-3",
          )
        )
    }
  }

  @Test
  fun `throws exception if no media types`() {
    val eventGroup = eventGroup {
      eventGroupReferenceId = "reference-id-4"
      this.eventGroupMetadata = eventGroupMetadata {
        this.adMetadata = adMetadata {
          this.campaignMetadata = campaignMetadata {
            brand = "brand-2"
            campaign = "campaign-2"
          }
        }
      }
      measurementConsumer = "measurement-consumer-2"
      dataAvailabilityInterval = interval {
        startTime = timestamp { seconds = 200 }
        endTime = timestamp { seconds = 300 }
      }
      mediaTypes += emptyList<MediaType>()
    }
    assertFailsWith<IllegalStateException> { EventGroupSync.validateEventGroup(eventGroup) }
  }

  @Test
  fun `throws exception if no event group reference id`() {
    val eventGroup = eventGroup {
      eventGroupReferenceId = ""
      this.eventGroupMetadata = eventGroupMetadata {
        this.adMetadata = adMetadata {
          this.campaignMetadata = campaignMetadata {
            brand = "brand-2"
            campaign = "campaign-2"
          }
        }
      }
      measurementConsumer = "measurement-consumer-2"
      dataAvailabilityInterval = interval {
        startTime = timestamp { seconds = 200 }
        endTime = timestamp { seconds = 300 }
      }
      mediaTypes += listOf(MediaType.valueOf("OTHER"))
    }
    assertFailsWith<IllegalStateException> { EventGroupSync.validateEventGroup(eventGroup) }
  }

  @Test
  fun `throw exceptions if do data availability`() {
    val eventGroup = eventGroup {
      eventGroupReferenceId = "some-event-group-reference-id"
      this.eventGroupMetadata = eventGroupMetadata {
        this.adMetadata = adMetadata {
          this.campaignMetadata = campaignMetadata {
            brand = "brand-2"
            campaign = "campaign-2"
          }
        }
      }
      measurementConsumer = "measurement-consumer-2"
      mediaTypes += listOf(MediaType.valueOf("OTHER"))
    }
    assertFailsWith<IllegalStateException> { EventGroupSync.validateEventGroup(eventGroup) }
  }

  @Test
  fun `throws exception if no event group metadata`() {
    val eventGroup = eventGroup {
      eventGroupReferenceId = "event-group-reference-id"
      measurementConsumer = "measurement-consumer-2"
      dataAvailabilityInterval = interval {
        startTime = timestamp { seconds = 200 }
        endTime = timestamp { seconds = 300 }
      }
      mediaTypes += listOf(MediaType.valueOf("OTHER"))
    }
    assertFailsWith<IllegalStateException> { EventGroupSync.validateEventGroup(eventGroup) }
  }

  @Test
  fun `throws exception if empty measurement consumer`() {
    val eventGroup = eventGroup {
      eventGroupReferenceId = "some-reference-id"
      this.eventGroupMetadata = eventGroupMetadata {
        this.adMetadata = adMetadata {
          this.campaignMetadata = campaignMetadata {
            brand = "brand-2"
            campaign = "campaign-2"
          }
        }
      }
      dataAvailabilityInterval = interval {
        startTime = timestamp { seconds = 200 }
        endTime = timestamp { seconds = 300 }
      }
      mediaTypes += listOf(MediaType.valueOf("OTHER"))
    }
    assertFailsWith<IllegalStateException> { EventGroupSync.validateEventGroup(eventGroup) }
  }

  companion object {
    private val CAMPAIGNS =
      listOf(
        eventGroup {
          eventGroupReferenceId = "reference-id-1"
          measurementConsumer = "measurementConsumers/measurement-consumer-1"
          this.eventGroupMetadata = eventGroupMetadata {
            this.adMetadata = adMetadata {
              this.campaignMetadata = campaignMetadata {
                brand = "brand-1"
                campaign = "campaign-1"
              }
            }
          }
          dataAvailabilityInterval = interval {
            startTime = timestamp { seconds = 200 }
            endTime = timestamp { seconds = 300 }
          }
          mediaTypes += listOf(MediaType.valueOf("VIDEO"), MediaType.valueOf("DISPLAY"))
        },
        eventGroup {
          eventGroupReferenceId = "reference-id-2"
          this.eventGroupMetadata = eventGroupMetadata {
            this.adMetadata = adMetadata {
              this.campaignMetadata = campaignMetadata {
                brand = "brand-2"
                campaign = "campaign-2"
              }
            }
          }
          measurementConsumer = "measurementConsumers/measurement-consumer-2"
          dataAvailabilityInterval = interval {
            startTime = timestamp { seconds = 200 }
            endTime = timestamp { seconds = 300 }
          }
          mediaTypes += listOf(MediaType.valueOf("OTHER"))
        },
        eventGroup {
          eventGroupReferenceId = "reference-id-3"
          this.eventGroupMetadata = eventGroupMetadata {
            this.adMetadata = adMetadata {
              this.campaignMetadata = campaignMetadata {
                brand = "brand-2"
                campaign = "campaign-3"
              }
            }
          }
          measurementConsumer = "measurementConsumers/measurement-consumer-2"
          dataAvailabilityInterval = interval {
            startTime = timestamp { seconds = 200 }
            endTime = timestamp { seconds = 300 }
          }
          mediaTypes += listOf(MediaType.valueOf("OTHER"))
        },
      )
  }
}
