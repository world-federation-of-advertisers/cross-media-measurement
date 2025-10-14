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
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.Tracer
import java.util.logging.Logger
import kotlin.time.TimeSource
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.edpaggregator.telemetry.withSpan
import org.wfanet.measurement.api.v2alpha.EventGroup as CmmsEventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataKt as CmmsEventGroupMetadataKt
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataKt.AdMetadataKt as CmmsAdMetadataKt
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MediaType as CmmsMediaType
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createEventGroupRequest
import org.wfanet.measurement.api.v2alpha.eventGroup as cmmsEventGroup
import org.wfanet.measurement.api.v2alpha.eventGroupMetadata as cmmsEventGroupMetadata
import org.wfanet.measurement.api.v2alpha.listEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.updateEventGroupRequest
import org.wfanet.measurement.common.Instrumentation
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
  private val tracer: Tracer = GlobalOpenTelemetry.getTracer("wfa.edpa"),
) {
  private val metrics = EventGroupSyncMetrics(Instrumentation.meter)

  /** Creates metric attributes with data provider name. */
  private fun metricAttributes() = Attributes.of(
    AttributeKey.stringKey("data_provider_name"), edpName
  )

  suspend fun sync(): Flow<MappedEventGroup> = flow {
    withSpan(
      tracer,
      "EventGroupSync",
      Attributes.of(
        AttributeKey.stringKey("data_provider_name"), edpName,
        AttributeKey.stringKey("source"), "kingdom"
      ),
      errorMessage = "EventGroupSync failed"
    ) { _ ->
      val syncedEventGroups: Map<String, CmmsEventGroup> =
        fetchEventGroups().toList().associateBy { it.eventGroupReferenceId }

      eventGroups.collect { eventGroup: EventGroup ->
        syncEventGroupItem(eventGroup, syncedEventGroups)?.let { emit(it) }
      }
    }
  }

  /**
   * Syncs a single event group item.
   *
   * @param eventGroup The event group to sync
   * @param syncedEventGroups A map of event group reference IDs to CmmsEventGroups
   * @return MappedEventGroup if sync succeeds, null if sync fails
   */
  private suspend fun syncEventGroupItem(
    eventGroup: EventGroup,
    syncedEventGroups: Map<String, CmmsEventGroup>
  ): MappedEventGroup? {
    val eventGroupRefId = eventGroup.eventGroupReferenceId

    return try {
      withSpan(
        tracer,
        "EventGroupSync.Item",
        Attributes.of(
          AttributeKey.stringKey("event_group_reference_id"), eventGroupRefId,
          AttributeKey.stringKey("data_provider_name"), edpName
        ),
        errorMessage = "Event Group sync failed"
      ) { _ ->
        // Start timing for sync latency
        val syncStartTime = TimeSource.Monotonic.markNow()

        // Record sync attempt
        metrics.syncAttempts.add(1, metricAttributes())

        validateEventGroup(eventGroup)
        val syncedEventGroup: CmmsEventGroup =
          if (eventGroup.eventGroupReferenceId in syncedEventGroups) {
            val existingEventGroup: CmmsEventGroup =
              syncedEventGroups.getValue(eventGroup.eventGroupReferenceId)
            val updatedEventGroup: CmmsEventGroup = updateEventGroup(existingEventGroup, eventGroup)
            if (updatedEventGroup != existingEventGroup) {
              updateCmmsEventGroup(updatedEventGroup)
            } else {
              existingEventGroup
            }
          } else {
            createCmmsEventGroup(edpName, eventGroup)
          }

        // Record sync success and latency
        metrics.syncSuccess.add(1, metricAttributes())

        val syncLatency = syncStartTime.elapsedNow().inWholeMilliseconds / 1000.0
        metrics.syncLatency.record(syncLatency, metricAttributes())

        mappedEventGroup {
          eventGroupReferenceId = syncedEventGroup.eventGroupReferenceId
          eventGroupResource = syncedEventGroup.name
        }
      }
    } catch (e: Exception) {
      if (e is CancellationException) throw e

      // Record sync failure
      metrics.syncFailure.add(1, metricAttributes())

      logger.severe(
        "Unable to process Event Group ${eventGroup.eventGroupReferenceId}: ${e.message}"
      )
      // Note: sync attempt was already recorded, but no success/latency on failure
      null
    }
  }

  /*
   * Updates the Cmms Public API with a [CmmsEventGroup].
   */
  private suspend fun updateCmmsEventGroup(eventGroup: CmmsEventGroup): CmmsEventGroup {
    return throttler.onReady {
      eventGroupsStub.updateEventGroup(updateEventGroupRequest { this.eventGroup = eventGroup })
    }
  }

  /*
   * Calls the Cmms Public API to create a [CmmsEventGroup] from an [EventGroup].
   */
  private suspend fun createCmmsEventGroup(
    edpName: String,
    eventGroup: EventGroup,
  ): CmmsEventGroup {
    val request = createEventGroupRequest {
      parent = edpName
      this.eventGroup = cmmsEventGroup {
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
        mediaTypes += eventGroup.mediaTypesList.map { it.toCmmsMediaType() }
        dataAvailabilityInterval = eventGroup.dataAvailabilityInterval
      }
    }
    return throttler.onReady { eventGroupsStub.createEventGroup(request) }
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
