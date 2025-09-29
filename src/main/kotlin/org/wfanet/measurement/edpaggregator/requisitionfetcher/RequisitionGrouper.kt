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
import org.wfanet.measurement.api.v2alpha.unpack
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
   * Validation result of a single [Requisition].
   *
   * - [VALID]: The requisition passed validation and can be included in processing.
   * - [INVALID]: The requisition failed validation and should be refused.
   */
  enum class RequisitionValidationStatus { VALID, INVALID }


  /**
   * Wraps a single [Requisition] with its validation status.
   *
   * @property requisition The underlying [Requisition] proto.
   * @property status Whether the requisition is [VALID] or [INVALID].
   * @property refusal If [status] is [INVALID], contains the refusal details to record.
   */
  data class RequisitionWrapper(
    val requisition: Requisition,
    val status: RequisitionValidationStatus,
    val refusal: Requisition.Refusal? = null
  )

  /**
   * Represents all requisitions grouped under the same `reportId`.
   *
   * A single [GroupedRequisitionsWrapper] corresponds to one report and contains:
   * - A [groupedRequisitions] proto message with only the **valid** requisitions,
   *   ready for storage and downstream fulfillment. This may be `null` if no
   *   requisitions were valid for the report.
   * - A list of [RequisitionWrapper]s for **all requisitions** (both valid and
   *   invalid) associated with the report. This allows creating metadata for
   *   every requisition and issuing refusals for the invalid ones.
   *
   * @property reportId The report identifier from `MeasurementSpec.reportingMetadata.report`.
   * @property groupedRequisitions A combined [GroupedRequisitions] proto containing only the valid requisitions,
   *   or `null` if all requisitions in the group are invalid.
   * @property requisitions The per-requisition wrappers (valid and invalid) for this report.
   */
  data class GroupedRequisitionsWrapper(
    val reportId: String,
    val groupedRequisitions: GroupedRequisitions? = null,
    val requisitions: List<RequisitionWrapper>
  )

  /**
   * Groups a list of disparate [Requisition] objects by their report ID.
   *
   * Each [Requisition] is first validated and mapped into a single-entry [GroupedRequisitionsWrapper].
   * Then, all requisitions for the same report are combined into a single wrapper. The combined
   * [GroupedRequisitionsWrapper] contains:
   * - [GroupedRequisitionsWrapper.groupedRequisitions]: a [GroupedRequisitions] proto with only the
   *   valid requisitions for that report (or `null` if none are valid).
   * - [GroupedRequisitionsWrapper.requisitions]: a list of [RequisitionWrapper]s representing both
   *   valid and invalid requisitions, with invalid ones carrying refusal information.
   *
   * @param requisitions The list of [Requisition] protos to be grouped.
   * @return A list of [GroupedRequisitionsWrapper], one per unique report.
   */
  suspend fun groupRequisitions(
    requisitions: List<Requisition>
  ): List<GroupedRequisitionsWrapper> {
    val mappedRequisitions: List<GroupedRequisitionsWrapper> = requisitions.mapNotNull { mapRequisition(it) }
    return combineGroupedRequisitions(mappedRequisitions)
  }

  /** Function to be implemented to combine [GroupedRequisition]s for optimal execution. */
  protected suspend abstract fun combineGroupedRequisitions(
    groupedRequisitions: List<GroupedRequisitionsWrapper>
  ): List<GroupedRequisitionsWrapper>

  /**
   * Maps a single [Requisition] into a [GroupedRequisitionsWrapper].
   *
   * This performs validation and mapping for one requisition:
   * - If the [MeasurementSpec] is invalid, the requisition cannot be grouped
   *   (no report ID can be derived), and `null` is returned.
   * - If the [RequisitionSpec] is invalid, a wrapper is returned containing the
   *   requisition marked as [RequisitionValidationStatus.INVALID] with the refusal reason.
   * - If event group mapping fails (e.g. storage or lookup errors), the requisition
   *   is skipped and `null` is returned.
   * - Otherwise, a [GroupedRequisitionsWrapper] is returned with:
   *   - [GroupedRequisitionsWrapper.groupedRequisitions] containing the proto with the single valid requisition.
   *   - [GroupedRequisitionsWrapper.requisitions] containing one [RequisitionWrapper] marked as valid.
   *
   * @param requisition The [Requisition] proto to validate and map.
   * @return A [GroupedRequisitionsWrapper] containing the requisition and its validation
   *   status, or `null` if the requisition must be skipped (invalid MeasurementSpec or
   *   event group mapping failure).
   */
  private suspend fun mapRequisition(requisition: Requisition): GroupedRequisitionsWrapper? {
    val measurementSpec: MeasurementSpec =
      try {
        requisitionValidator.validateMeasurementSpec(requisition)
      } catch (e: InvalidRequisitionException) {
        logger.severe(
          "Exception getting measurement spec for requisition ${requisition.name}: ${e.message}"
        )
        // Cannot be grouped since MeasurementSpec is invalid, therefore no groupID available to create the RequisitionMetadata.
        return null
      }

    val reportId = getReportId(requisition)

    val requisitionSpec: RequisitionSpec =
      try {
        requisitionValidator.validateRequisitionSpec(requisition)
      } catch (e: InvalidRequisitionException) {
        return GroupedRequisitionsWrapper(
          reportId = reportId,
          groupedRequisitions = null,
          requisitions = listOf(
            RequisitionWrapper(requisition, RequisitionValidationStatus.INVALID, e.refusal)
          )
        )
      }
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
    val groupedRequisitions = groupedRequisitions {
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
    return GroupedRequisitionsWrapper(
      reportId = reportId,
      groupedRequisitions,
      requisitions = listOf(
        RequisitionWrapper(requisition, RequisitionValidationStatus.VALID)
      )
    )
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

  private fun getReportId(requisition: Requisition): String {
    return requisition.measurementSpec
      .unpack<MeasurementSpec>().reportingMetadata.report
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
