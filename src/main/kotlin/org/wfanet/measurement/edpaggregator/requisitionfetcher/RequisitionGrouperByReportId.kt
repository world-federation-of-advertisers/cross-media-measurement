/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.wfanet.measurement.edpaggregator.requisitionfetcher

import com.google.protobuf.Any
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
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt.eventGroupDetails
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt.eventGroupMapEntry
import org.wfanet.measurement.edpaggregator.v1alpha.groupedRequisitions

/**
 * Combines a single report's already-validated [Requisition]s into one [GroupedRequisitions].
 *
 * This grouper is a pure, in-memory transform. It does not call the Requisition Metadata Service,
 * it does not generate the group ID, and it assumes its caller has already performed report-level
 * validation (model-line consistency, requisition-spec decryption). Per-requisition Kingdom
 * refusals triggered by [InconsistentEventGroupSelectorsException] are the caller's responsibility.
 */
class RequisitionGrouperByReportId(
  requisitionValidator: RequisitionsValidator,
  requisitionsClient: RequisitionsCoroutineStub,
  eventGroupsClient: EventGroupsCoroutineStub,
  kingdomMutationThrottler: Throttler,
  kingdomEventGroupThrottler: Throttler,
) :
  RequisitionGrouper(
    requisitionValidator,
    requisitionsClient,
    eventGroupsClient,
    kingdomMutationThrottler,
    kingdomEventGroupThrottler,
  ) {

  /**
   * Groups [requisitions] for [reportId] into a single [GroupedRequisitions] tagged with [groupId].
   * Returns `null` if [requisitions] is empty.
   *
   * Throws [InconsistentEventGroupSelectorsException] when the resolved event groups have mixed
   * entity-key / reference-id presence. Throws if a per-requisition spec cannot be decrypted; the
   * caller is expected to have pre-validated the inputs.
   */
  suspend fun groupForReport(
    reportId: String,
    requisitions: List<Requisition>,
    groupId: String,
  ): GroupedRequisitions? {
    if (requisitions.isEmpty()) return null
    val perRequisitionGroups =
      requisitions.map { req ->
        val measurementSpec: MeasurementSpec = req.measurementSpec.unpack()
        val eventGroupMapEntries =
          getEventGroupMapEntries(requisitionValidator.getRequisitionSpec(req))
        groupedRequisitions {
          modelLine = measurementSpec.modelLine
          this.requisitions +=
            GroupedRequisitionsKt.requisitionEntry { requisition = Any.pack(req) }
          this.eventGroupMap +=
            eventGroupMapEntries.map { (eventGroupName, details) ->
              eventGroupMapEntry {
                eventGroup = eventGroupName
                this.details = details
              }
            }
        }
      }
    return mergeGroupedRequisitions(reportId, perRequisitionGroups, groupId)
  }

  /**
   * Merges per-requisition [GroupedRequisitions] entries into a single [GroupedRequisitions] under
   * [groupId]. Assumes all entries share the same model line; collection intervals on each event
   * group are unioned via [unionIntervals]. [reportId] is threaded through for error context only.
   */
  private fun mergeGroupedRequisitions(
    reportId: String,
    perRequisitionGroups: List<GroupedRequisitions>,
    groupId: String,
  ): GroupedRequisitions {
    val firstModelLine = perRequisitionGroups.first().modelLine
    val mergedRequisitions = perRequisitionGroups.flatMap { it.requisitionsList }
    val eventGroupMapEntries = buildEventGroupEntries(reportId, perRequisitionGroups)
    return groupedRequisitions {
      modelLine = firstModelLine
      requisitions += mergedRequisitions
      eventGroupMap += eventGroupMapEntries
      this.groupId = groupId
    }
  }

  /**
   * Combines event-group map entries across [groups], merging overlapping collection intervals and
   * verifying that `event_group_reference_id` and `entity_key` are consistent for each event group.
   * [reportId] is included in `require()` failure messages for triage context.
   */
  private fun buildEventGroupEntries(
    reportId: String,
    groups: List<GroupedRequisitions>,
  ): List<EventGroupMapEntry> =
    groups
      .flatMap { it.eventGroupMapList }
      .groupBy { it.eventGroup }
      .map { (eventGroupName, entries) ->
        val allDetails = entries.map { it.details }

        val refIds = allDetails.map { it.eventGroupReferenceId }.distinct()
        require(refIds.size == 1) {
          "Inconsistent event_group_reference_id for $eventGroupName in report $reportId: $refIds"
        }

        val entityKeys = allDetails.map { it.entityKey }.distinct()
        require(entityKeys.size == 1) {
          "Inconsistent entity_key for $eventGroupName in report $reportId: $entityKeys"
        }

        val intervals = allDetails.flatMap { it.collectionIntervalsList }
        val merged = unionIntervals(intervals)
        eventGroupMapEntry {
          eventGroup = eventGroupName
          details = eventGroupDetails {
            eventGroupReferenceId = refIds.single()
            collectionIntervals += merged
            if (allDetails.first().hasEntityKey()) {
              entityKey = entityKeys.single()
            }
          }
        }
      }

  /**
   * Merges overlapping or contiguous [intervals] into the minimal set of non-overlapping intervals,
   * sorted by start time.
   */
  private fun unionIntervals(intervals: List<Interval>): List<Interval> {
    if (intervals.isEmpty()) return emptyList()
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
