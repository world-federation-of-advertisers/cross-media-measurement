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

import io.grpc.StatusException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2alpha.EventGroup as ExternalEventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataKt.AdMetadataKt.campaignMetadata as externalCampaignMetadata
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataKt.adMetadata as externalAdMetadata
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MediaType as ExternalMediaType
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createEventGroupRequest
import org.wfanet.measurement.api.v2alpha.eventGroup as externalEventGroup
import org.wfanet.measurement.api.v2alpha.eventGroupMetadata as externalEventGroupMetadata
import org.wfanet.measurement.api.v2alpha.listEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.updateEventGroupRequest
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.flattenConcat
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.MappedEventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.mappedEventGroup

/*
 * Syncs event groups with kingdom.
 * 1. Registers any unregistered event groups
 * 2. Updates any existing event groups if data has changed
 * 2. Returns a flow of event_group_reference_id to EventGroup
 */
class EventGroupSync(
  private val edpName: String,
  private val eventGroupsStub: EventGroupsCoroutineStub,
  private val eventGroups: Flow<EventGroup>,
  private val throttler: Throttler,
) {

  suspend fun sync(): Flow<MappedEventGroup> = flow {
    val syncedEventGroups: Map<String, ExternalEventGroup> =
      fetchEventGroups().toList().associateBy { it.eventGroupReferenceId }
    eventGroups.collect { eventGroup: EventGroup ->
      val syncedEventGroup =
        if (eventGroup.eventGroupReferenceId in syncedEventGroups) {
          val existingEventGroup = syncedEventGroups[eventGroup.eventGroupReferenceId]!!
          val updatedEventGroup = updateEventGroup(existingEventGroup, eventGroup)
          if (!updatedEventGroup.equals(existingEventGroup)) {
            updateKingdomEventGroup(existingEventGroup, eventGroup)
          } else {
            existingEventGroup
          }
        } else {
          createKingdomEventGroup(edpName, eventGroup)
        }
      emit(
        mappedEventGroup {
          eventGroupReferenceId = syncedEventGroup.eventGroupReferenceId
          eventGroupResource = syncedEventGroup.name
        }
      )
    }
  }

  private suspend fun updateKingdomEventGroup(existingEventGroup: ExternalEventGroup, eventGroup: EventGroup): ExternalEventGroup {
    val request = updateEventGroupRequest {
      this.eventGroup =
        existingEventGroup.copy {
          measurementConsumer = eventGroup.measurementConsumer
          eventGroupReferenceId = eventGroup.eventGroupReferenceId
          this.eventGroupMetadata = externalEventGroupMetadata {
            this.adMetadata = externalAdMetadata {
              this.campaignMetadata = externalCampaignMetadata {
                brandName = eventGroup.eventGroupMetadata.adMetadata.campaignMetadata.brand
                campaignName =
                  eventGroup.eventGroupMetadata.adMetadata.campaignMetadata.campaign
              }
            }
          }
          mediaTypes.clear()
          mediaTypes += eventGroup.mediaTypesList.map { ExternalMediaType.valueOf(it) }
          dataAvailabilityInterval = eventGroup.dataAvailabilityInterval
        }
    }
    return throttler.onReady { eventGroupsStub.updateEventGroup(request) }
  }

  private suspend fun createKingdomEventGroup(edpName: String, eventGroup: EventGroup): ExternalEventGroup {
    val request = createEventGroupRequest {
      parent = edpName
      this.eventGroup = externalEventGroup {
        measurementConsumer = eventGroup.measurementConsumer
        eventGroupReferenceId = eventGroup.eventGroupReferenceId
        this.eventGroupMetadata = externalEventGroupMetadata {
          this.adMetadata = externalAdMetadata {
            this.campaignMetadata = externalCampaignMetadata {
              brandName = eventGroup.eventGroupMetadata.adMetadata.campaignMetadata.brand
              campaignName =
                eventGroup.eventGroupMetadata.adMetadata.campaignMetadata.campaign
            }
          }
        }
        mediaTypes += eventGroup.mediaTypesList.map { ExternalMediaType.valueOf(it) }
        dataAvailabilityInterval = eventGroup.dataAvailabilityInterval
      }
    }
    return throttler.onReady { eventGroupsStub.createEventGroup(request) }
  }

  private fun updateEventGroup(existingEventGroup: ExternalEventGroup, eventGroup: EventGroup): ExternalEventGroup {
    return existingEventGroup.copy {
      measurementConsumer = eventGroup.measurementConsumer
      eventGroupReferenceId = eventGroup.eventGroupReferenceId
      this.eventGroupMetadata = externalEventGroupMetadata {
        this.adMetadata = externalAdMetadata {
          this.campaignMetadata = externalCampaignMetadata {
            brandName = eventGroup.eventGroupMetadata.adMetadata.campaignMetadata.brand
            campaignName =
              eventGroup.eventGroupMetadata.adMetadata.campaignMetadata.campaign
          }
        }
      }
      mediaTypes.clear()
      mediaTypes += eventGroup.mediaTypesList.map { ExternalMediaType.valueOf(it) }
      dataAvailabilityInterval = eventGroup.dataAvailabilityInterval
    }
  }

  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private fun fetchEventGroups(): Flow<ExternalEventGroup> {
    return eventGroupsStub
      .listResources { pageToken: String ->
        val response =
          try {
            throttler.onReady {
              eventGroupsStub.listEventGroups(
                listEventGroupsRequest {
                  parent = edpName
                  this.pageToken = pageToken
                }
              )
            }
          } catch (e: StatusException) {
            throw Exception("Error listing EventGroups", e)
          }
        ResourceList(response.eventGroupsList, response.nextPageToken)
      }
      .flattenConcat()
  }
}
