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
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions.RequisitionEntry
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt.eventGroupDetails
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt.eventGroupMapEntry
import org.wfanet.measurement.edpaggregator.v1alpha.groupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.requisitionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.createRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.refuseRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineStub
import java.util.UUID

/**
 * Groups requisitions by Report ID. Assumes that the collection intervals for a report are not
 * disparate.
 */
class RequisitionGrouperByReportId(
  private val requisitionValidator: RequisitionsValidator,
  private val dataProviderName: String,
  private val blobUriPrefix: String,
  private val requisitionMetadataStub: RequisitionMetadataServiceCoroutineStub,
  eventGroupsClient: EventGroupsCoroutineStub,
  requisitionsClient: RequisitionsCoroutineStub,
  throttler: Throttler,
) : RequisitionGrouper(requisitionValidator, eventGroupsClient, requisitionsClient, throttler) {

  /**
   * Combines Grouped Requisitions by ReportId and then unions their collection intervals per event
   * group.
   */
  override suspend fun combineGroupedRequisitions(
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
  private suspend fun combineByReportId(
    groupedByReport: Map<String, List<GroupedRequisitions>>
  ): List<GroupedRequisitions> {

    return groupedByReport.toList().mapNotNull {
      (reportId: String, groups: List<GroupedRequisitions>) ->
      try {
        requisitionValidator.validateModelLines(groups, reportId = reportId)
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
        val requisitions: List<RequisitionEntry> = groups.flatMap { it.requisitionsList }
        // TODO(world-federation-of-advertisers/cross-media-measurement#2987): Use batch create once available
        for (requisition in requisitions) {
          createRequisitionMetadata(requisition.requisition.unpack(Requisition::class.java))
        }
        groupedRequisitions {
          this.modelLine = groups.firstOrNull()?.modelLine ?: ""
          this.eventGroupMap += entries
          this.requisitions += groups.flatMap { it.requisitionsList }
        }
      } catch (e: InvalidRequisitionException) {
        e.requisitions.forEach {
          refuseRequisition(it, e.refusal)
//          val requisitionMetadata: RequisitionMetadata = createRequisitionMetadata(it)
//          refuseRequisitionMetadata(requisitionMetadata, e.refusal.message)
        }
        null
      }
    }
  }

  private suspend fun createRequisitionMetadata(requisition: Requisition): RequisitionMetadata {

    val requisitionGroupId = UUID.randomUUID().toString()
    val requisitionBlobUri = "$blobUriPrefix/$requisitionGroupId"
    val reportId = getReportId(requisition)

    val metadata = requisitionMetadata {
      cmmsRequisition = requisition.name
      blobUri = requisitionBlobUri
      blobTypeUrl = GROUPED_REQUISITION_BLOB_TYPE_URL
      groupId = requisitionGroupId
      cmmsCreateTime = requisition.updateTime
      this.report = reportId
    }
    val request = createRequisitionMetadataRequest {
      parent = dataProviderName
      requisitionMetadata = metadata
      requestId = requisitionGroupId
    }
    return requisitionMetadataStub.createRequisitionMetadata(request)
  }

  private suspend fun refuseRequisitionMetadata(requisitionMetadata: RequisitionMetadata, message: String) {
    val request = refuseRequisitionMetadataRequest {
      name = requisitionMetadata.name
      etag = requisitionMetadata.etag
      refusalMessage = message
    }
    requisitionMetadataStub.refuseRequisitionMetadata(request)
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

  private fun getReportId(requisition: Requisition): String {
    return requisition.measurementSpec
      .unpack<MeasurementSpec>().reportingMetadata.report
  }

  companion object {
    private const val GROUPED_REQUISITION_BLOB_TYPE_URL = "type.googleapis.com/wfa.measurement.edpaggregator.v1alpha.GroupedRequisitions"
  }
}
