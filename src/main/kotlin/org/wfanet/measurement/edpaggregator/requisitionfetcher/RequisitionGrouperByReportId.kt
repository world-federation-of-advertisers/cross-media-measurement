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

import com.google.protobuf.Timestamp
import com.google.type.Interval
import com.google.type.interval
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.Requisition.Refusal
import org.wfanet.measurement.api.v2alpha.RequisitionKt.refusal
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.dataprovider.RequisitionRefusalException
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt.eventGroupMapEntry
import org.wfanet.measurement.edpaggregator.v1alpha.groupedRequisitions

/**
 * Groups requisitions by Report ID. Assumes that the collection intervals for a report are not
 * disparate.
 *
 * TODO: Consider additional grouping options like group by identical or overlapping time intervals
 *   only.
 */
class RequisitionGrouperByReportId(
  privateEncryptionKey: PrivateKeyHandle,
  eventGroupsClient: EventGroupsCoroutineStub,
  requisitionsClient: RequisitionsCoroutineStub,
  throttler: Throttler,
) : RequisitionGrouper(privateEncryptionKey, eventGroupsClient, requisitionsClient, throttler) {

  override fun combineGroupedRequisitions(
    groupedRequisitions: List<GroupedRequisitions>
  ): List<GroupedRequisitions> {
    println("~~~~~~~~~~~~~~~~ AAAAA1")
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
    println("~~~~~~~~~~~~~~~~ AAAAA2")
    val combinedByReportId =
      groupedByReport.toList().mapNotNull { (reportId: String, groups: List<GroupedRequisitions>) ->
        val sortedGroups: List<GroupedRequisitions> =
          groups.sortedBy {
            // Sort by the first event group's collection interval start
            it.collectionIntervals
              .toList()
              .sortedBy { it.first }
              .first()
              .second
              .startTime
              .toInstant()!!
          }
        val modelLine = groups.first().modelLine
        val combinedEventGroupMap = mutableMapOf<String, String>()
        val foundInvalidModelLine = sortedGroups.firstOrNull { it.modelLine != modelLine } != null
        if (foundInvalidModelLine) {
          logger.info("Report $reportId cannot contain multiple model lines")
          groups.forEach {
            val requisition =
              it.requisitionsList.single().requisition.unpack(Requisition::class.java)
            runBlocking {
              refuseRequisition(
                requisition.name,
                refusal {
                  justification = Requisition.Refusal.Justification.UNFULFILLABLE
                  message = "Report $reportId cannot contain multiple model lines"
                },
              )
            }
          }
        }
        sortedGroups.forEach {
          combinedEventGroupMap.putAll(
            it.eventGroupMapList.map { Pair(it.eventGroup, it.eventGroupReferenceId) }
          )
        }
        val combinedCollectionIntervalMap = mutableMapOf<String, Interval>()
        val ableToCombineCollectionIntervalMap =
          try {
            sortedGroups.forEach {
              combinedCollectionIntervalMap.mergeIntervals(it.collectionIntervals)
            }
            true
          } catch (e: RequisitionRefusalException) {
            logger.info("Report $reportId cannot contain disparate collection intervals")
            groups.forEach {
              val requisition =
                it.requisitionsList.single().requisition.unpack(Requisition::class.java)
              runBlocking {
                refuseRequisition(
                  requisition.name,
                  refusal {
                    justification = e.justification
                    message = e.message!!
                  },
                )
              }
            }
            false
          }
        if (foundInvalidModelLine || !ableToCombineCollectionIntervalMap) {
          null
        } else {
          groupedRequisitions {
            this.modelLine = modelLine
            this.eventGroupMap +=
              combinedEventGroupMap.toList().map {
                eventGroupMapEntry {
                  this.eventGroup = it.first
                  this.eventGroupReferenceId = it.second
                }
              }
            this.requisitions += groups.flatMap { it.requisitionsList }
            this.collectionIntervals.putAll(combinedCollectionIntervalMap)
          }
        }
      }
    return combinedByReportId
  }

  /** Returns null if the map cannot be combined. */
  private fun MutableMap<String, Interval>.mergeIntervals(
    intervals: Map<String, Interval>
  ): Map<String, Interval> {
    val combinedCollectionIntervalMap = this
    intervals.toList().forEach { (eventGroupReferenceId: String, collectionInterval: Interval) ->
      if (eventGroupReferenceId !in combinedCollectionIntervalMap) {
        combinedCollectionIntervalMap.put(eventGroupReferenceId, collectionInterval)
      } else {
        val newValue =
          combinedCollectionIntervalMap.getValue(eventGroupReferenceId).combine(collectionInterval)
            ?: throw RequisitionRefusalException.Default(
              justification = Requisition.Refusal.Justification.UNFULFILLABLE,
              message = "Report cannot contain multiple model lines",
            )
        combinedCollectionIntervalMap[eventGroupReferenceId] = newValue!!
      }
    }
    return combinedCollectionIntervalMap
  }

  private fun Timestamp.greaterThan(otherTimestamp: Timestamp): Boolean {
    return (seconds > otherTimestamp.seconds ||
      (seconds == otherTimestamp.seconds && nanos > otherTimestamp.nanos))
  }

  /** Returns null if the intervals are not combinable (non-overlapping). */
  private fun Interval.combine(otherInterval: Interval): Interval? {
    // Get the earliest start time
    val startA = startTime
    val startB = otherInterval.startTime
    // Get the latest end time
    val endA = endTime
    val endB = otherInterval.endTime

    if (startA.greaterThan(endB) || startB.greaterThan(endA)) {
      return null
    }
    val minStart = if (startA.greaterThan(startB)) startB else startA

    val maxEnd = if (endA.greaterThan(endB)) endA else endB
    return interval {
      startTime = minStart
      endTime = maxEnd
    }
  }
}
