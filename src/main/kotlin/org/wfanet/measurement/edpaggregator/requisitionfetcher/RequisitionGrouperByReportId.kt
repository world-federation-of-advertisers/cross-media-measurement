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

import com.google.type.Interval
import com.google.type.interval
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt.eventGroupDetails
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt.eventGroupMapEntry
import org.wfanet.measurement.edpaggregator.v1alpha.groupedRequisitions

/**
 * Groups requisitions by Report ID. Assumes that the collection intervals for a report are not
 * disparate.
 */
class RequisitionGrouperByReportId(
  private val requisitionValidator: RequisitionsValidator,
  eventGroupsClient: EventGroupsCoroutineStub,
  requisitionsClient: RequisitionsCoroutineStub,
  throttler: Throttler,
) : RequisitionGrouper(requisitionValidator, eventGroupsClient, requisitionsClient, throttler) {

  /**
   * Combines Grouped Requisitions by ReportId and then unions their collection intervals per event
   * group.
   */
  override suspend fun combineGroupedRequisitions(
    groupedRequisitions: List<GroupedRequisitionsWrapper>
  ): List<GroupedRequisitionsWrapper> {

    val groupedRequisitionByReportId = groupedRequisitions.groupBy { it.reportId }

    return groupedRequisitionByReportId.map { (reportId, groupedRequisitionsWrapper) ->
      val requisitionWrappers = groupedRequisitionsWrapper.flatMap { it.requisitions }.toMutableList()

      // Filter out invalid requisitions
      val validGroups = groupedRequisitionsWrapper
        .filter { it.requisitions.single().status == RequisitionValidationStatus.VALID }
        .mapNotNull { it.groupedRequisitions }

      if (validGroups.isNotEmpty()) {
        try {
          // If requisitions share the same model line, return the GroupRequisitions object that will be store to cloud storage.
          val combinedValidReports = combineByReportId(reportId, validGroups)
          GroupedRequisitionsWrapper(
            reportId = reportId,
            groupedRequisitions = combinedValidReports,
            requisitions = requisitionWrappers
          )
        } catch (e: InvalidRequisitionException) {
          val updatedRequisitionWrappers = requisitionWrappers.map {
            if (it.status == RequisitionValidationStatus.VALID) {
              it.copy(status = RequisitionValidationStatus.INVALID, refusal = e.refusal)
            } else it
          }
          // Invalid model line: nothing to be stored to cloud storage for this group
          GroupedRequisitionsWrapper(
            reportId = reportId,
            groupedRequisitions = null,
            requisitions = updatedRequisitionWrappers
          )
        }
      } else {
        GroupedRequisitionsWrapper(
          reportId = reportId,
          groupedRequisitions = null,
          requisitions = requisitionWrappers
        )
      }
    }
  }

  /**
   * Combines Grouped Requisitions by ReportId and then unions their collection intervals per event
   * group.
   */
  private fun combineByReportId(
    reportId: String,
    groups: List<GroupedRequisitions>
  ): GroupedRequisitions {

    requisitionValidator.validateModelLines(groups, reportId = reportId)
    val entries =
      groups
        .flatMap { it.eventGroupMapList }
        .groupBy { it.eventGroup }
        .map { (eventGroupName, eventGroupMapEntries) ->
          val eventGroupReferenceId = eventGroupMapEntries.first().details.eventGroupReferenceId
          val collectionIntervals: List<Interval> =
            eventGroupMapEntries.flatMap { it.details.collectionIntervalsList }
          val combinedCollectionIntervals = unionIntervals(collectionIntervals)
          eventGroupMapEntry {
            this.eventGroup = eventGroupName
            details = eventGroupDetails {
              this.eventGroupReferenceId = eventGroupReferenceId
              this.collectionIntervals += combinedCollectionIntervals
            }
          }
        }
    return groupedRequisitions {
      this.modelLine = groups.first().modelLine
      this.eventGroupMap += entries
      this.requisitions += groups.flatMap { it.requisitionsList }
    }
  }

  private fun unionIntervals(intervals: List<Interval>): List<Interval> {
    val sorted = intervals.sortedBy { it.startTime.toInstant() }
    val result = mutableListOf<Interval>()
    var current = sorted.first()
    for (i in 1 until sorted.size) {
      val next = sorted[i]
      if (current.endTime.toInstant() >= next.startTime.toInstant()) {
        current = interval {
          startTime = current.startTime
          endTime = maxOf(current.endTime.toInstant(), next.endTime.toInstant()).toProtoTime()
        }
      } else {
        result.add(current)
        current = next
      }
    }
    result.add(current)
    return result
  }
}
