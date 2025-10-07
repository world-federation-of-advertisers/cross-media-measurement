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
import io.grpc.StatusException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.Requisition.Refusal
import org.wfanet.measurement.api.v2alpha.RequisitionKt.refusal
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.flattenConcat
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt.eventGroupDetails
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt.eventGroupMapEntry
import org.wfanet.measurement.edpaggregator.v1alpha.ListRequisitionMetadataRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListRequisitionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.createRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.groupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.listRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.refuseRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions.EventGroupMapEntry
import org.wfanet.measurement.edpaggregator.v1alpha.requisitionMetadata
import org.wfanet.measurement.storage.StorageClient
import java.util.UUID
import java.util.logging.Logger

data class EventGroupWrapper(val eventGroupReferenceId: String, val intervals: MutableList<Interval>)

/**
 * Groups requisitions by Report ID. Assumes that the collection intervals for a report are not
 * disparate.
 */
class RequisitionGrouperByReportId(
  private val requisitionValidator: RequisitionsValidator,
  private val dataProviderName: String,
  private val blobUriPrefix: String,
  private val requisitionMetadataStub: RequisitionMetadataServiceCoroutineStub,
  private val storageClient: StorageClient,
  private val responsePageSize: Int? = null,
  private val storagePathPrefix: String,
  throttler: Throttler,
  eventGroupsClient: EventGroupsCoroutineStub,
  requisitionsClient: RequisitionsCoroutineStub,
) : RequisitionGrouper(requisitionValidator, requisitionsClient, eventGroupsClient, throttler) {

  /**
   * Combines Grouped Requisitions by ReportId and then unions their collection intervals per event
   * group.
   */
  override suspend fun combineGroupedRequisitions(
    groupedRequisitions: List<GroupedRequisitions>,
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
    return groupedByReport.toList().flatMap { (reportId, groups) ->
      val results = mutableListOf<GroupedRequisitions>()
      val requisitionGroupId = UUID.randomUUID().toString()

      // List existing requisition metadata for the current report
      val existingRequisitionMetadata: List<RequisitionMetadata> = listRequisitionMetadataByReportId(reportId)
      val existingCmmsRequisitions = existingRequisitionMetadata.map { it.cmmsRequisition }.toSet()
      if (existingRequisitionMetadata.isNotEmpty()) {
        results.addAll(getUnwrittenRequisitions(existingRequisitionMetadata, groups))
      }
      // Filter out groups whose single requisitions has already persisted to RequisitionMetadata storage.
      val filteredGroups = groups.filter { group ->
        val requisition = group.requisitionsList.single().requisition.unpack(Requisition::class.java)
        requisition.name !in existingCmmsRequisitions
      }

      if (filteredGroups.isEmpty()) {
        return@flatMap results
      }

      val requisitions = filteredGroups.flatMap { it.requisitionsList }

      try {

        requisitionValidator.validateModelLines(filteredGroups, reportId = reportId)

        val entries = buildEventGroupEntries(filteredGroups)

        // TODO(world-federation-of-advertisers/cross-media-measurement#2987): Use batch create once
        // available
        // Create requisition metadata for requisition that were not created already
        for (requisition in requisitions) {
          createRequisitionMetadata(
            requisition.requisition.unpack(Requisition::class.java),
            requisitionGroupId,
          )
        }

        val newGroupedRequisitions = groupedRequisitions {
          this.modelLine = filteredGroups.firstOrNull()?.modelLine ?: ""
          this.eventGroupMap += entries
          this.requisitions += requisitions
          this.groupId = requisitionGroupId
        }

        results.add(newGroupedRequisitions)
      } catch (e: InvalidRequisitionException) {
        logger.info("Invalid requisition exception: $e")
        refuseAllRequisitions(requisitions, requisitionGroupId, existingCmmsRequisitions, e.refusal.justification, e.message ?: "Invalid requisition")
      } catch (e: Exception) {
        logger.info("Error while grouping requisitions: $e")
        refuseAllRequisitions(requisitions, requisitionGroupId, existingCmmsRequisitions, Refusal.Justification.DECLINED)
      }

      results

    }
  }

  private suspend fun getUnwrittenRequisitions(
    existingRequisitionMetadata: List<RequisitionMetadata>,
    groups: List<GroupedRequisitions>
  ): List<GroupedRequisitions> {
    // Requisition metadata may already have stored multiple groups for the same report
    val requisitionMetadataByGroupId = existingRequisitionMetadata.groupBy { it.groupId }
    val fixedGroupedRequisitions = mutableListOf<GroupedRequisitions>()

    for ((reqMetadataGroupId, metadataRequisitionssForGroup) in requisitionMetadataByGroupId) {
      val storedGroupedRequisition: GroupedRequisitions? = readGroupedRequisitionBlob(reqMetadataGroupId)

      if (storedGroupedRequisition != null) continue

      val existingCmmsRequisitionNames: Set<String> = metadataRequisitionssForGroup.map { it.cmmsRequisition }.toSet()

      val missingGroupedRequisitions: List<GroupedRequisitions> =
        groups.filter { group ->
          val entry = group.requisitionsList.single()
          entry.requisition.unpack(Requisition::class.java).name in existingCmmsRequisitionNames
        }

      if (missingGroupedRequisitions.isEmpty()) {
        logger.info("GroupedRequisitions blob not found. Unable to create it with existing unfulfilled requisitions.")
        continue
      }

      val combinedRequisitions = missingGroupedRequisitions.flatMap { it.requisitionsList }
      val combinedEventGroupMap = buildEventGroupEntries(missingGroupedRequisitions)

      val existingModelLine = missingGroupedRequisitions.firstOrNull()?.modelLine.orEmpty()

      fixedGroupedRequisitions += groupedRequisitions {
        modelLine = existingModelLine
        eventGroupMap += combinedEventGroupMap
        requisitions += combinedRequisitions
        groupId = reqMetadataGroupId
      }

    }
    return fixedGroupedRequisitions
  }

  private fun buildEventGroupEntries(groups: List<GroupedRequisitions>): List<EventGroupMapEntry> =
    groups
      .flatMap { it.eventGroupMapList }
      .groupBy { it.eventGroup }
      .map { (eventGroupName, entries) ->
        val refId = entries.first().details.eventGroupReferenceId
        val intervals = entries.flatMap { it.details.collectionIntervalsList }
        val merged = unionIntervals(intervals) // same function you already have
        eventGroupMapEntry {
          eventGroup = eventGroupName
          details = eventGroupDetails {
            eventGroupReferenceId = refId
            collectionIntervals += merged
          }
        }
      }

  /**
   * Merges overlapping or contiguous time intervals into a minimal set of non-overlapping intervals.
   *
   * @param intervals The list of [Interval] objects to be merged.
   * @return A list of merged, non-overlapping [Interval] objects, sorted by start time.
   */
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

  private suspend fun readGroupedRequisitionBlob(groupId: String) : GroupedRequisitions? {
    val blobKey = "$storagePathPrefix/${groupId}"
    val blob = storageClient.getBlob(blobKey) ?: return null
    return GroupedRequisitions.parseFrom(blob.read().flatten())
  }

  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private suspend fun listRequisitionMetadataByReportId(reportName: String): List<RequisitionMetadata> {
    val requisitionMetadataList: Flow<RequisitionMetadata> =
      requisitionMetadataStub
        .listResources { pageToken: String ->
          val request = listRequisitionMetadataRequest {
            parent = dataProviderName
            filter = ListRequisitionMetadataRequestKt.filter { report = reportName }
            if (responsePageSize != null) {
              pageSize = responsePageSize
            }
            this.pageToken = pageToken
          }
          val response: ListRequisitionMetadataResponse =
            try {
              requisitionMetadataStub.listRequisitionMetadata(request)
            } catch (e: StatusException) {
              throw Exception("Error listing requisitions", e)
            }
          ResourceList(response.requisitionMetadataList, response.nextPageToken)
        }
        .flattenConcat()
    return requisitionMetadataList.toList()
  }

  private suspend fun refuseAllRequisitions(
    requisitions: List<GroupedRequisitions.RequisitionEntry>,
    requisitionGroupId: String,
    existingCmmsRequisitions: Collection<String>,
    refusalJustification: Refusal.Justification,
    refusalMessage: String = ""
  ) {

    requisitions.forEach { entry ->
      val requisition = entry.requisition.unpack(Requisition::class.java)

      // Skip if requisition already exists in metadata storage
      if (requisition.name in existingCmmsRequisitions) return@forEach

      val refusal = refusal {
        justification = refusalJustification
        message = refusalMessage
      }

      refuseRequisition(requisition, refusal)
      val requisitionMetadata = createRequisitionMetadata(requisition, requisitionGroupId)
      refuseRequisitionMetadata(requisitionMetadata, refusalMessage)
    }
  }

  private suspend fun createRequisitionMetadata(
    requisition: Requisition,
    requisitionGroupId: String,
  ): RequisitionMetadata {

    val requisitionBlobUri = "$blobUriPrefix/$storagePathPrefix/$requisitionGroupId"
    val reportId = getReportId(requisition)

    val metadata = requisitionMetadata {
      cmmsRequisition = requisition.name
      blobUri = requisitionBlobUri
      blobTypeUrl = GROUPED_REQUISITION_BLOB_TYPE_URL
      groupId = requisitionGroupId
      cmmsCreateTime = requisition.updateTime
      this.report = reportId
    }
    val createRequisitionMetadataRequestId = UUID.randomUUID().toString()
    val request = createRequisitionMetadataRequest {
      parent = dataProviderName
      requisitionMetadata = metadata
      requestId = createRequisitionMetadataRequestId
    }
    return requisitionMetadataStub.createRequisitionMetadata(request)
  }

  private suspend fun refuseRequisitionMetadata(
    requisitionMetadata: RequisitionMetadata,
    message: String,
  ) {
    val request = refuseRequisitionMetadataRequest {
      name = requisitionMetadata.name
      etag = requisitionMetadata.etag
      refusalMessage = message
    }
    requisitionMetadataStub.refuseRequisitionMetadata(request)
  }

  private fun getReportId(requisition: Requisition): String {
    return requisition.measurementSpec.unpack<MeasurementSpec>().reportingMetadata.report
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private val GROUPED_REQUISITION_BLOB_TYPE_URL =
      ProtoReflection.getTypeUrl(GroupedRequisitions.getDescriptor())
  }
}
