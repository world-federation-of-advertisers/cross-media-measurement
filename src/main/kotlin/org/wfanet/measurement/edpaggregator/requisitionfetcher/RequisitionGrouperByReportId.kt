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
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions.EventGroupMapEntry
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
  override fun combineGroupedRequisitions(
    groupedRequisitions: List<GroupedRequisitions>
  ): List<GroupedRequisitions> {
    val groupedByReport: Map<String, List<GroupedRequisitions>> =
      groupedRequisitions.groupBy {
        val measurementSpec: MeasurementSpec =
          it.requisitionsList
            .single()
            .requisition
            .unpack(Requisition::class.java)
            .measurementSpec
            .unpack()
        measurementSpec.reportingMetadata.report
      }
    val combinedByReportId: List<GroupedRequisitions> = combineByReportId(groupedByReport)
    return combinedByReportId
  }

  /**
   * Combines Grouped Requisitions by ReportId and then unions their collection intervals per event
   * group.
   */
  private fun combineByReportId(
    groupedByReport: Map<String, List<GroupedRequisitions>>
  ): List<GroupedRequisitions> {
    return groupedByReport.toList().mapNotNull {
      (reportId: String, groups: List<GroupedRequisitions>) ->
      if (!requisitionValidator.validateModelLines(groups, reportId = reportId)) {
        null
      } else {
        val entries =
          groups
            .flatMap { it.eventGroupMapList }
            .groupBy { it.eventGroup }
            .map { (eventGroupName: String, eventGroupMapEntries: List<EventGroupMapEntry>) ->
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
        groupedRequisitions {
          this.modelLine = modelLine
          this.eventGroupMap += entries
          this.requisitions += groups.flatMap { it.requisitionsList }
        }
      }
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
