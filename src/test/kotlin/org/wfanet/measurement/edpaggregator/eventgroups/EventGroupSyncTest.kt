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
import org.wfanet.measurement.api.v2alpha.EventGroup as ExternalEventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataKt.AdMetadataKt.campaignMetadata as externalCampaignMetadata
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataKt.adMetadata as externalAdMetadata
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.MediaType as ExternalMediaType
import org.wfanet.measurement.api.v2alpha.UpdateEventGroupRequest
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.eventGroup as externalEventGroup
import org.wfanet.measurement.api.v2alpha.eventGroupMetadata as externalEventGroupMetadata
import org.wfanet.measurement.api.v2alpha.listEventGroupsResponse
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.EventGroupMetadataKt.AdMetadataKt.campaignMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.EventGroupMetadataKt.adMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.eventGroupMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.eventGroup

@RunWith(JUnit4::class)
class EventGroupSyncTest {

  private val campaigns =
    listOf(
      eventGroup {
        eventGroupReferenceId = "reference-id-1"
        measurementConsumer = "measurement-consumer-1"
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
        mediaTypes += listOf("VIDEO", "DISPLAY")
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
        measurementConsumer = "measurement-consumer-2"
        dataAvailabilityInterval = interval {
          startTime = timestamp { seconds = 200 }
          endTime = timestamp { seconds = 300 }
        }
        mediaTypes += listOf("OTHER")
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
        measurementConsumer = "measurement-consumer-2"
        dataAvailabilityInterval = interval {
          startTime = timestamp { seconds = 200 }
          endTime = timestamp { seconds = 300 }
        }
        mediaTypes += listOf("OTHER")
      },
    )

  private val eventGroupsServiceMock: EventGroupsCoroutineImplBase = mockService {
    onBlocking { updateEventGroup(any<UpdateEventGroupRequest>()) }
      .thenAnswer { invocation -> invocation.getArgument<UpdateEventGroupRequest>(0).eventGroup }
    onBlocking { createEventGroup(any<CreateEventGroupRequest>()) }
      .thenAnswer { invocation -> invocation.getArgument<CreateEventGroupRequest>(0).eventGroup }
    onBlocking { listEventGroups(any<ListEventGroupsRequest>()) }
      .thenAnswer {
        listEventGroupsResponse {
          eventGroups += campaigns[0].toEventGroup()
          eventGroups += campaigns[1].toEventGroup()
          eventGroups +=
            campaigns[2].toEventGroup().copy {
              this.eventGroupMetadata = externalEventGroupMetadata {
                this.adMetadata = externalAdMetadata {
                  this.campaignMetadata = externalCampaignMetadata { brandName = "new-brand-name" }
                }
              }
            }
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
      mediaTypes += listOf("OTHER")
    }
    val testCampaigns = campaigns + newCampaign
    val eventGroupSync = EventGroupSync("edp-name", eventGroupsStub, testCampaigns.asFlow())
    runBlocking { eventGroupSync.sync().collect() }
    verifyBlocking(eventGroupsServiceMock, times(1)) { createEventGroup(any()) }
  }

  @Test
  fun `sync updatesExistingEventGroups`() {
    val eventGroupSync = EventGroupSync("edp-name", eventGroupsStub, campaigns.asFlow())
    runBlocking { eventGroupSync.sync().collect() }
    verifyBlocking(eventGroupsServiceMock, times(1)) { updateEventGroup(any()) }
  }

  @Test
  fun sync_returnsMapOfEventGroupReferenceIdsToEventGroups() {
    runBlocking {
      val eventGroupSync = EventGroupSync("edp-name", eventGroupsStub, campaigns.asFlow())
      val result = runBlocking { eventGroupSync.sync() }
      assertThat(result.toList().map { it.eventGroupReferenceId to it.eventGroupResourceName })
        .isEqualTo(
          listOf(
            "reference-id-1" to "resource-name-for-reference-id-1",
            "reference-id-2" to "resource-name-for-reference-id-2",
            "reference-id-3" to "resource-name-for-reference-id-3",
          )
        )
    }
  }
}

private fun EventGroup.toEventGroup(): ExternalEventGroup {
  val campaign = this
  return externalEventGroup {
    name = "resource-name-for-${campaign.eventGroupReferenceId}"
    measurementConsumer = campaign.measurementConsumer
    eventGroupReferenceId = campaign.eventGroupReferenceId
    this.eventGroupMetadata = externalEventGroupMetadata {
      this.adMetadata = externalAdMetadata {
        this.campaignMetadata = externalCampaignMetadata {
          brandName = campaign.eventGroupMetadata.adMetadata.campaignMetadata.brand
          campaignName = campaign.eventGroupMetadata.adMetadata.campaignMetadata.campaign
        }
      }
    }
    mediaTypes += campaign.mediaTypesList.map { ExternalMediaType.valueOf(it) }
    dataAvailabilityInterval = campaign.dataAvailabilityInterval
  }
}
