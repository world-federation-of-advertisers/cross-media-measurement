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

import com.google.type.interval
import io.grpc.StatusException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataKt.AdMetadataKt.campaignMetadata
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataKt.adMetadata
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MediaType
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createEventGroupRequest
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.eventGroupMetadata
import org.wfanet.measurement.api.v2alpha.listEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.updateEventGroupRequest
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.flattenConcat
import org.wfanet.measurement.common.api.grpc.listResources

/*
 * Syncs event groups with kingdom.
 * 1. Registers any unregistered event groups
 * 2. Updates any existing event groups if data has changed
 * 2. Returns a map of event_group_reference_id to EventGroup
 */
class EventGroupSync(
  private val edpName: String,
  private val eventGroupsStub: EventGroupsCoroutineStub,
  private val campaigns: List<CampaignMetadata>,
) {
  suspend fun sync(): Map<String, String> {
    val registeredEventGroups: Map<String, EventGroup> =
      fetchEventGroups().toList().associateBy { it.eventGroupReferenceId }
    val eventGroupMap = mutableMapOf<String, String>()
    for (campaign in campaigns) {
      val eventGroup =
        if (campaign.eventGroupReferenceId in registeredEventGroups) {
          val existingEventGroup = registeredEventGroups[campaign.eventGroupReferenceId]!!
          val syncedEventGroup =
            existingEventGroup.copy {
              measurementConsumer = campaign.measurementConsumerName
              eventGroupReferenceId = campaign.eventGroupReferenceId
              this.eventGroupMetadata = eventGroupMetadata {
                this.adMetadata = adMetadata {
                  this.campaignMetadata = campaignMetadata {
                    brandName = campaign.brandName
                    campaignName = campaign.campaignName
                  }
                }
              }
              mediaTypes.clear()
              mediaTypes += campaign.mediaTypesList.map { MediaType.valueOf(it) }
              dataAvailabilityInterval = interval {
                startTime = campaign.startTime
                endTime = campaign.endTime
              }
            }
          if (!syncedEventGroup.equals(existingEventGroup)) {
            val request = updateEventGroupRequest {
              eventGroup =
                existingEventGroup.copy {
                  measurementConsumer = campaign.measurementConsumerName
                  eventGroupReferenceId = campaign.eventGroupReferenceId
                  this.eventGroupMetadata = eventGroupMetadata {
                    this.adMetadata = adMetadata {
                      this.campaignMetadata = campaignMetadata {
                        brandName = campaign.brandName
                        campaignName = campaign.campaignName
                      }
                    }
                  }
                  mediaTypes.clear()
                  mediaTypes += campaign.mediaTypesList.map { MediaType.valueOf(it) }
                  dataAvailabilityInterval = interval {
                    startTime = campaign.startTime
                    endTime = campaign.endTime
                  }
                }
            }
            eventGroupsStub.updateEventGroup(request)
          } else {
            existingEventGroup
          }
        } else {
          val request = createEventGroupRequest {
            parent = edpName
            eventGroup = eventGroup {
              measurementConsumer = campaign.measurementConsumerName
              eventGroupReferenceId = campaign.eventGroupReferenceId
              this.eventGroupMetadata = eventGroupMetadata {
                this.adMetadata = adMetadata {
                  this.campaignMetadata = campaignMetadata {
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

          eventGroupsStub.createEventGroup(request)
        }
      eventGroupMap[campaign.eventGroupReferenceId] = eventGroup.name
    }
    return eventGroupMap
  }

  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private fun fetchEventGroups(): Flow<EventGroup> {
    return eventGroupsStub
      .listResources { pageToken ->
        val response =
          try {
            eventGroupsStub.listEventGroups(
              listEventGroupsRequest {
                parent = edpName
                this.pageToken = pageToken
              }
            )
          } catch (e: StatusException) {
            throw Exception("Error listing EventGroups", e)
          }
        ResourceList(response.eventGroupsList, response.nextPageToken)
      }
      .flattenConcat()
  }
}
