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
import java.util.logging.Logger
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2alpha.EventGroup as CmmsEventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataKt as CmmsEventGroupMetadataKt
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataKt.AdMetadataKt as CmmsAdMetadataKt
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.batchCreateEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.batchUpdateEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.MediaType as CmmsMediaType
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createEventGroupRequest
import org.wfanet.measurement.api.v2alpha.eventGroup as cmmsEventGroup
import org.wfanet.measurement.api.v2alpha.eventGroupMetadata as cmmsEventGroupMetadata
import org.wfanet.measurement.api.v2alpha.listEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.updateEventGroupRequest
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.flattenConcat
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup.MediaType
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.MappedEventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.mappedEventGroup

/*
 * Syncs event groups with the CMMS Public API.
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

  /**
   * Synchronizes EventGroups between the EDP and the CMMS public API.
   *
   * This function:
   * 1. Fetches all existing [CmmsEventGroup]s from the CMMS API for the given [edpName].
   * 2. Validates and categorizes incoming [EventGroup]s from the [eventGroups] flow:
   *    - Creates new CMMS EventGroups for any that don't yet exist.
   *    - Updates existing CMMS EventGroups when metadata has changed.
   *    - Keeps existing CMMS EventGroups unchanged otherwise.
   * 3. Performs batched create and update operations (up to 50 per batch).
   * 4. Emits a [MappedEventGroup] for every input event group (created, updated, or unchanged).
   *
   * @return A [Flow] emitting one [MappedEventGroup].
   * @throws Exception if there is a failure when listing, creating, or updating event groups via gRPC.
   */
  suspend fun sync(): Flow<MappedEventGroup> = flow {
    val existingEventGroups: Map<String, CmmsEventGroup> =
      fetchEventGroups().toList().associateBy { it.eventGroupReferenceId }

    val eventGroupsToCreate = mutableListOf<EventGroup>()
    val eventGroupsToUpdate = mutableListOf<CmmsEventGroup>()
    val eventGroupsUnchanged = mutableListOf<CmmsEventGroup>()

    val allEventGroups = eventGroups.toList()
    allEventGroups.forEach { eventGroup ->
      try {
        validateEventGroup(eventGroup)
        val existing = existingEventGroups[eventGroup.eventGroupReferenceId]
        if (existing == null) {
          eventGroupsToCreate += eventGroup
        } else {
          val updatedEventGroup = updateEventGroup(existing, eventGroup)
          if (updatedEventGroup != existing) {
            eventGroupsToUpdate += updatedEventGroup
          }else {
            eventGroupsUnchanged += existing
          }
        }
      } catch (e: Exception) {
        logger.severe(
          "Unable to process Event Group ${eventGroup.eventGroupReferenceId}: ${e.message}"
        )
      }
    }

    // Perform batched create/update
    val createdGroups = batchCreateEventGroups(edpName, eventGroupsToCreate)
    val updatedGroups = batchUpdateEventGroups(edpName, eventGroupsToUpdate)

    val syncedByRefId =
      (existingEventGroups.values + createdGroups + updatedGroups)
        .associateBy { it.eventGroupReferenceId }


    allEventGroups.forEach { eventGroup ->
      val synced = syncedByRefId[eventGroup.eventGroupReferenceId]
      if (synced != null) {
        emit(
          mappedEventGroup {
            eventGroupReferenceId = synced.eventGroupReferenceId
            eventGroupResource = synced.name
          }
        )
      } else {
        logger.warning(
          "No CMMS EventGroup found for ${eventGroup.eventGroupReferenceId}"
        )
      }
    }

  }

  /*
   * Returns a copy of a [CmmsEventGroup] with information from an [EventGroup].
   * Used to determine if a CmmsEventGroup needs updating.
   */
  private fun updateEventGroup(
    existingEventGroup: CmmsEventGroup,
    eventGroup: EventGroup,
  ): CmmsEventGroup {
    return existingEventGroup.copy {
      measurementConsumer = eventGroup.measurementConsumer
      eventGroupReferenceId = eventGroup.eventGroupReferenceId
      this.eventGroupMetadata = cmmsEventGroupMetadata {
        this.adMetadata =
          CmmsEventGroupMetadataKt.adMetadata {
            this.campaignMetadata =
              CmmsAdMetadataKt.campaignMetadata {
                brandName = eventGroup.eventGroupMetadata.adMetadata.campaignMetadata.brand
                campaignName = eventGroup.eventGroupMetadata.adMetadata.campaignMetadata.campaign
              }
          }
      }
      mediaTypes.clear()
      mediaTypes += eventGroup.mediaTypesList.map { it.toCmmsMediaType() }
      dataAvailabilityInterval = eventGroup.dataAvailabilityInterval
    }
  }

  /**
   * Creates multiple [CmmsEventGroup]s in the CMMS API in batches of up to 50 items.
   *
   * For each [EventGroup] in [eventGroups], this function builds a corresponding
   * [CreateEventGroupRequest] and groups them into a [BatchCreateEventGroupsRequest].
   * The gRPC `BatchCreateEventGroups` method is then invoked for each batch, using the provided [throttler]
   * to control request concurrency.
   *
   * If [eventGroups] is empty, the function returns an empty list without making any gRPC calls.
   *
   * @param edpName The full resource name of the parent DataProvider (e.g. `"dataProviders/123"`).
   * @param eventGroups The list of event groups to be created in CMMS.
   * @return A list of successfully created [CmmsEventGroup]s returned by the CMMS API.
   * @throws io.grpc.StatusException If any of the batch create operations fail at the gRPC level.
   */
  private suspend fun batchCreateEventGroups(
    edpName: String,
    eventGroups: List<EventGroup>
  ): List<CmmsEventGroup> {
    if (eventGroups.isEmpty()) return emptyList()

    return eventGroups.chunked(50).flatMap { chunk ->
      val requests = chunk.map { eg: EventGroup ->
        createEventGroupRequest {
          parent = edpName
          this.eventGroup = cmmsEventGroup {
            measurementConsumer = eg.measurementConsumer
            eventGroupReferenceId = eg.eventGroupReferenceId
            this.eventGroupMetadata = cmmsEventGroupMetadata {
              this.adMetadata =
                CmmsEventGroupMetadataKt.adMetadata {
                  this.campaignMetadata =
                    CmmsAdMetadataKt.campaignMetadata {
                      brandName = eg.eventGroupMetadata.adMetadata.campaignMetadata.brand
                      campaignName = eg.eventGroupMetadata.adMetadata.campaignMetadata.campaign
                    }
                }
            }
            mediaTypes += eg.mediaTypesList.map { it.toCmmsMediaType() }
            dataAvailabilityInterval = eg.dataAvailabilityInterval
          }
        }
      }

      val batchRequest = batchCreateEventGroupsRequest {
        this.parent = parent
        this.requests += requests
      }

      throttler.onReady {
        eventGroupsStub.batchCreateEventGroups(batchRequest).eventGroupsList
      }
    }
  }

  /**
   * Updates multiple [CmmsEventGroup]s in the CMMS API in batches of up to 50 items.
   *
   * For each [CmmsEventGroup] in [eventGroups], this function builds an [UpdateEventGroupRequest]
   * and groups them into a [BatchUpdateEventGroupsRequest]. Each batch is sent via the CMMS gRPC
   * `BatchUpdateEventGroups` method, and throttled by [throttler] to limit request rate.
   *
   * If [eventGroups] is empty, the function returns an empty list without making any gRPC calls.
   *
   * @param parent The full resource name of the parent DataProvider (e.g. `"dataProviders/123"`).
   * @param eventGroups The list of CMMS event groups to update.
   * @return A list of updated [CmmsEventGroup]s as returned by the CMMS API.
   * @throws io.grpc.StatusException If any of the batch update operations fail at the gRPC level.
   */
  private suspend fun batchUpdateEventGroups(
    parent: String,
    eventGroups: List<CmmsEventGroup>
  ): List<CmmsEventGroup> {
    if (eventGroups.isEmpty()) return emptyList()

    return eventGroups.chunked(50).flatMap { chunk ->
      val requests = chunk.map { eg ->
        updateEventGroupRequest { this.eventGroup = eg }
      }

      val batchRequest = batchUpdateEventGroupsRequest {
        this.parent = parent
        this.requests += requests
      }

      throttler.onReady {
        eventGroupsStub.batchUpdateEventGroups(batchRequest).eventGroupsList
      }
    }
  }

  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private fun fetchEventGroups(): Flow<CmmsEventGroup> {
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

  private fun MediaType.toCmmsMediaType(): CmmsMediaType {
    return when (this) {
      MediaType.MEDIA_TYPE_UNSPECIFIED -> error("Media type must be set")
      MediaType.VIDEO -> CmmsMediaType.VIDEO
      MediaType.DISPLAY -> CmmsMediaType.DISPLAY
      MediaType.OTHER -> CmmsMediaType.OTHER
      MediaType.UNRECOGNIZED -> error("Not a real media type")
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    /*
     * Validates that event groups fields are populated
     * Throws exceptions for any invalid fields.
     */
    fun validateEventGroup(eventGroup: EventGroup) {
      check(eventGroup.mediaTypesList.size > 0) { "At least one media type must be set" }
      check(eventGroup.hasDataAvailabilityInterval()) { "Data availability must be set" }
      check(eventGroup.hasEventGroupMetadata()) { "Event Group Metadata must be set" }
      check(eventGroup.eventGroupReferenceId.isNotBlank()) {
        "Event Group Reference Id must be set"
      }
      check(eventGroup.measurementConsumer.isNotBlank()) { "Measurement Consumer must be set" }
    }
  }
}
