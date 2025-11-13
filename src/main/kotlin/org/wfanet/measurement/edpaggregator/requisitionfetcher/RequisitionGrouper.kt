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

package org.wfanet.measurement.edpaggregator.requisitionfetcher

import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Level
import java.util.logging.Logger
import org.wfanet.measurement.api.v2alpha.EventGroup as CmmsEventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.getEventGroupRequest
import org.wfanet.measurement.api.v2alpha.refuseRequisitionRequest
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions.EventGroupDetails
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt.eventGroupDetails

/**
 * Abstract base class for grouping and validating [Requisition]s before aggregation.
 *
 * This class defines the core workflow for transforming raw [Requisition] objects into grouped
 * forms ([GroupedRequisitions]) ready for execution. Subclasses implement specific grouping logic
 * (e.g., by report ID).
 *
 * @param requisitionValidator Validates requisitions and their measurement specs.
 * @param requisitionsClient gRPC client for updating or refusing requisitions in the Kingdom.
 * @param eventGroupsClient gRPC client for retrieving event group metadata.
 * @param throttler Used to control concurrency for gRPC calls.
 */
abstract class RequisitionGrouper(
  private val requisitionValidator: RequisitionsValidator,
  private val requisitionsClient: RequisitionsCoroutineStub,
  private val eventGroupsClient: EventGroupsCoroutineStub,
  private val throttler: Throttler,
) {

  private val cmmsEventGroupMap: MutableMap<String, CmmsEventGroup> = ConcurrentHashMap()

  /**
   * Groups a list of [Requisition]s into [GroupedRequisitions]s suitable for execution.
   *
   * ### High-Level Flow
   * 1. Each requisition is validated via [mapRequisition].
   * 2. Invalid requisitions are refused to the Cmms and excluded.
   * 3. Valid requisitions are passed to [createGroupedRequisitions] for combination.
   *
   * @param requisitions Input requisitions to group.
   * @return A list of grouped requisitions, excluding any refused entries.
   */
  suspend fun groupRequisitions(requisitions: List<Requisition>): List<GroupedRequisitions> {
    val mappedRequisitions = requisitions.mapNotNull { mapRequisition(it) }
    return createGroupedRequisitions(mappedRequisitions)
  }

  /**
   * Abstract method for combining validated [Requisition]s into [GroupedRequisitions].
   *
   * Implementations define the grouping strategy (e.g., by report ID) and handle additional
   * persistence of metadata logic.
   */
  protected suspend abstract fun createGroupedRequisitions(
    requisitions: List<Requisition>
  ): List<GroupedRequisitions>

  /**
   * Validates and maps a single [Requisition] into a form ready for grouping.
   *
   * ### High-Level Flow
   * 1. Validate the requisition’s [MeasurementSpec].
   * 2. On success, return the requisition for grouping.
   * 3. On failure, refuse the requisition via [refuseRequisitionToCmms].
   *
   * Requisitions without a valid [MeasurementSpec] or `reportId` are **not persisted** in the
   * metadata store, since a valid `reportId` is required for persistence.
   *
   * @param requisition The requisition to validate.
   * @return The validated requisition, or `null` if refused.
   */
  private suspend fun mapRequisition(requisition: Requisition): Requisition? {
    try {
      requisitionValidator.validateMeasurementSpec(requisition)
      return requisition
    } catch (e: InvalidRequisitionException) {
      refuseRequisitionToCmms(e.requisitions.single(), e.refusal)
      return null
    }
  }

  /**
   * Builds a map of event group details for a requisition.
   *
   * ### High-Level Flow
   * 1. Iterates over all event group references in the [RequisitionSpec].
   * 2. Fetches event group metadata from the Kingdom using [EventGroupsCoroutineStub].
   * 3. Aggregates collection intervals per event group, merging and sorting by start time.
   *
   * @param requisitionSpec The requisition specification to resolve event groups from.
   * @return A map from event group names to their [EventGroupDetails].
   */
  protected suspend fun getEventGroupMapEntries(
    requisitionSpec: RequisitionSpec
  ): Map<String, EventGroupDetails> {
    val eventGroupMap = mutableMapOf<String, EventGroupDetails>()
    for (eventGroupEntry in requisitionSpec.events.eventGroupsList) {
      val eventGroupName = eventGroupEntry.key
      if (eventGroupName in eventGroupMap) {
        eventGroupMap[eventGroupName] =
          eventGroupMap
            .getValue(eventGroupName)
            .toBuilder()
            .apply {
              val newCollectionIntervalList =
                this.collectionIntervalsList + eventGroupEntry.value.collectionInterval
              this.collectionIntervalsList.clear()
              this.collectionIntervalsList +=
                newCollectionIntervalList.sortedBy { it.startTime.toInstant() }
            }
            .build()
      } else {
        eventGroupMap[eventGroupName] = eventGroupDetails {
          val eventGroup = getEventGroup(eventGroupName)
          this.eventGroupReferenceId = eventGroup.eventGroupReferenceId
          this.collectionIntervals += eventGroupEntry.value.collectionInterval
        }
      }
    }
    return eventGroupMap
  }

  /**
   * Returns the [CmmsEventGroup] for the given resource name.
   *
   * This method uses a local cache to avoid repeated lookups. If the event group is already cached,
   * the cached value is returned immediately. Otherwise, the event group is retrieved from CMMS
   * using a throttled gRPC request, stored in the cache, and then returned.
   *
   * The call suspends while waiting for throttler capacity and the remote gRPC request to complete.
   *
   * @param name The full resource name of the event group.
   * @return The corresponding [CmmsEventGroup], either cached or freshly fetched.
   */
  private suspend fun getEventGroup(name: String): CmmsEventGroup {
    return cmmsEventGroupMap[name]
      ?: run {
        throttler.onReady {
          cmmsEventGroupMap[name] =
            eventGroupsClient.getEventGroup(getEventGroupRequest { this.name = name })
        }
        cmmsEventGroupMap.getValue(name)
      }
  }

  /**
   * Refuses a requisition to the Cmms.
   *
   * ### High-Level Flow
   * 1. Logs the refusal locally.
   * 2. Sends a [refuseRequisitionRequest] via [RequisitionsCoroutineStub].
   * 3. Errors during refusal are caught and logged.
   *
   * @param requisition The requisition to refuse.
   * @param refusal The reason and message for the refusal.
   */
  protected suspend fun refuseRequisitionToCmms(
    requisition: Requisition,
    refusal: Requisition.Refusal,
  ) {
    try {
      throttler.onReady {
        logger.info("Requisition ${requisition.name} was refused. $refusal")
        val request = refuseRequisitionRequest {
          this.name = requisition.name
          this.refusal =
            RequisitionKt.refusal {
              justification = refusal.justification
              message = refusal.message
            }
        }
        requisitionsClient.refuseRequisition(request)
      }
    } catch (e: Exception) {
      logger.log(Level.SEVERE, "Error while refusing requisition ${requisition.name}", e)
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
