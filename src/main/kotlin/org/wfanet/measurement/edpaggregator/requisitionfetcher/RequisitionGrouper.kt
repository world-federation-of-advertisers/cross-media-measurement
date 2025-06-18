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

import com.google.protobuf.Any
import io.grpc.StatusException
import java.util.logging.Logger
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.getEventGroupRequest
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions.EventGroupDetails
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt.eventGroupDetails
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt.eventGroupMapEntry
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt.requisitionEntry
import org.wfanet.measurement.edpaggregator.v1alpha.groupedRequisitions

/**
 * An interface to group a list of requisitions.
 *
 * This class provides functionality to categorize a collection of [Requisition] objects into
 * groups, facilitating efficient execution.
 *
 * @param requisitionValidator: The [RequisitionValidator] to use to validate the requisition.
 * @param eventGroupsClient The gRPC client used to interact with event groups.
 * @param requisitionsClient The gRPC client used to interact with requisitions.
 * @param throttler used to throttle gRPC requests
 */
abstract class RequisitionGrouper(
  private val requisitionValidator: RequisitionsValidator,
  private val eventGroupsClient: EventGroupsCoroutineStub,
  private val requisitionsClient: RequisitionsCoroutineStub,
  private val throttler: Throttler,
) {

  /**
   * Groups a list of disparate [Requisition] objects for execution.
   *
   * This method takes in a list of [Requisition] objects, maps them to their respective groups, and
   * then combines these groups into a single list of [GroupedRequisitions].
   *
   * @param requisitions A list of [Requisition] objects to be grouped.
   * @return A list of [GroupedRequisitions] containing the categorized [Requisition] objects.
   */
  suspend fun groupRequisitions(requisitions: List<Requisition>): List<GroupedRequisitions> {
    val mappedRequisitions = requisitions.mapNotNull { mapRequisition(it) }
    return combineGroupedRequisitions(mappedRequisitions)
  }

  /** Function to be implemented to combine [GroupedRequisition]s for optimal execution. */
  protected abstract fun combineGroupedRequisitions(
    groupedRequisitions: List<GroupedRequisitions>
  ): List<GroupedRequisitions>

  /* Maps a single [Requisition] to a single [GroupedRequisition]. */
  private suspend fun mapRequisition(requisition: Requisition): GroupedRequisitions? {

    val measurementSpec: MeasurementSpec =
      requisitionValidator.validateMeasurementSpec(requisition) ?: return null
    val requisitionSpec: RequisitionSpec =
      requisitionValidator.validateRequisitionSpec(requisition) ?: return null
    val eventGroupMapEntries =
      try {
        getEventGroupMapEntries(requisitionSpec)
      } catch (e: StatusException) {
        logger.severe(
          "Exception getting event group map for requisition ${requisition.name}: ${e.message}"
        )
        // For now, we skip this requisition. However, we could refuse it in the future.
        return null
      }
    return groupedRequisitions {
      modelLine = measurementSpec.modelLine
      this.requisitions += requisitionEntry { this.requisition = Any.pack(requisition) }
      this.eventGroupMap +=
        eventGroupMapEntries.map {
          eventGroupMapEntry {
            this.eventGroup = it.key
            details = it.value
          }
        }
    }
  }

  private suspend fun getEventGroup(name: String): EventGroup {
    return throttler.onReady {
      eventGroupsClient.getEventGroup(getEventGroupRequest { this.name = name })
    }
  }

  private suspend fun getEventGroupMapEntries(
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

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
