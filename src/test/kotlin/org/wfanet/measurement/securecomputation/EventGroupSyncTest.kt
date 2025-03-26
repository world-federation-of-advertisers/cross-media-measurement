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

package org.wfanet.measurement.securecomputation

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.timestamp
import com.google.type.interval
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.api.v2alpha.CreateEventGroupRequest
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataKt.AdMetadataKt.campaignMetadata as eventGroupCampaignMetadata
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataKt.adMetadata
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.MediaType
import org.wfanet.measurement.api.v2alpha.UpdateEventGroupRequest
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.eventGroupMetadata
import org.wfanet.measurement.api.v2alpha.listEventGroupsResponse
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService

// import com.google.common.truth.extensions.proto.ProtoTruth.assertThat

@RunWith(JUnit4::class)
class EventGroupSyncTest {

  private val campaigns =
    listOf(
      campaignMetadata {
        eventGroupReferenceId = "reference-id-1"
        campaignName = "campaign-1"
        measurementConsumerName = "measurement-consumer-1"
        brandName = "brand-1"
        startTime = timestamp { seconds = 200 }
        endTime = timestamp { seconds = 300 }
        mediaTypes += listOf("VIDEO", "DISPLAY")
      },
      campaignMetadata {
        eventGroupReferenceId = "reference-id-2"
        campaignName = "campaign-2"
        measurementConsumerName = "measurement-consumer-2"
        brandName = "brand-2"
        startTime = timestamp { seconds = 200 }
        endTime = timestamp { seconds = 300 }
        mediaTypes += listOf("OTHER")
      },
      campaignMetadata {
        eventGroupReferenceId = "reference-id-3"
        campaignName = "campaign-2"
        measurementConsumerName = "measurement-consumer-2"
        brandName = "brand-2"
        startTime = timestamp { seconds = 200 }
        endTime = timestamp { seconds = 300 }
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
              this.eventGroupMetadata = eventGroupMetadata {
                this.adMetadata = adMetadata {
                  this.campaignMetadata = eventGroupCampaignMetadata {
                    brandName = "new-brand-name"
                  }
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
    val testCampaigns =
      campaigns + campaigns[0].copy { eventGroupReferenceId = "some-new-reference-id" }
    val eventGroupSync = EventGroupSync("edp-name", eventGroupsStub, testCampaigns)
    runBlocking { eventGroupSync.sync() }
    verifyBlocking(eventGroupsServiceMock, times(1)) { createEventGroup(any()) }
  }

  @Test
  fun `sync updatesExistingEventGroups`() {
    val eventGroupSync = EventGroupSync("edp-name", eventGroupsStub, campaigns)
    runBlocking { eventGroupSync.sync() }
    verifyBlocking(eventGroupsServiceMock, times(1)) { updateEventGroup(any()) }
  }

  @Test
  fun sync_returnsMapOfEventGroupReferenceIdsToEventGroups() {
    runBlocking {
      val eventGroupSync = EventGroupSync("edp-name", eventGroupsStub, campaigns)
      val result = runBlocking { eventGroupSync.sync() }
      assertThat(result)
        .isEqualTo(
          mapOf(
            "reference-id-1" to "resource-name-for-reference-id-1",
            "reference-id-2" to "resource-name-for-reference-id-2",
            "reference-id-3" to "resource-name-for-reference-id-3",
          )
        )
    }
  }
}

private fun CampaignMetadata.toEventGroup(): EventGroup {
  val campaign = this
  return eventGroup {
    name = "resource-name-for-${campaign.eventGroupReferenceId}"
    measurementConsumer = campaign.measurementConsumerName
    eventGroupReferenceId = campaign.eventGroupReferenceId
    this.eventGroupMetadata = eventGroupMetadata {
      this.adMetadata = adMetadata {
        this.campaignMetadata = eventGroupCampaignMetadata {
          brandName = campaign.brandName
          campaignName = campaign.campaignName
        }
      }
    }
    mediaTypes += campaign.mediaTypesList.map { MediaType.valueOf(it) }
    dataAvailabilityInterval = interval {
      startTime = campaign.startTime
      endTime = campaign.endTime
    }
  }
}
