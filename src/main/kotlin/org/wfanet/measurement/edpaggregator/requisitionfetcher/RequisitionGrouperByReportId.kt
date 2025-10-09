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
import com.google.protobuf.Any
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
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt
import org.wfanet.measurement.edpaggregator.v1alpha.requisitionMetadata
import org.wfanet.measurement.storage.StorageClient
import java.util.UUID
import java.util.logging.Logger

data class GroupedRequisitionsWrapper(
  val groupedRequisitions: GroupedRequisitions?,
  val refusals: Map<Requisition, Refusal>
)

/**
 * Groups [Requisition]s by **Report ID** and manages their lifecycle in the Requisition Metadata Storage.
 *
 * This class aggregates all [Requisition]s associated with the same report into a single
 * [GroupedRequisitions] message. It ensures each group is validated, persisted, and recoverable
 * through its metadata records. Requisitions failing validation are refused both upstream (via the
 * Kingdom) and locally (via the Requisition Metadata Storage).
 *
 * ### High-Level Flow
 *
 * 1. **Group by Report ID** — Partition requisitions by their report field in
 *    [MeasurementSpec.reportingMetadata.report].
 *
 * 2. **List Existing Metadata** — Retrieve existing [RequisitionMetadata] entries for the report
 *    using the Requisition Metadata Storage (`requisitionMetadataStub.listRequisitionMetadata`).
 *
 * 3. **Recover Missing Groups** — For metadata entries whose blobs are missing in [StorageClient],
 *    re-create the grouped requisitions.
 *
 * 4. **Validate New Requisitions** — Validate unregistered requisitions. On failure, record
 *    refusals to the Kingdom and in metadata storage.
 *
 * 5. **Persist Metadata for New Groups** — Create new [RequisitionMetadata] entries via
 *    `requisitionMetadataStub.createRequisitionMetadata`, associating each requisition with a
 *    generated group ID and blob URI.
 *
 * @property requisitionValidator Validates that all grouped requisitions are consistent and compatible.
 * @property dataProviderName The name of the data provider resource, used as the parent in metadata requests.
 * @property blobUriPrefix Prefix URI used to construct blob paths for storing grouped requisitions.
 * @property requisitionMetadataStub Stub for communicating with the Requisition Metadata Service.
 * @property storageClient Client used to check for and read existing grouped requisition blobs.
 * @property responsePageSize Optional page size for listing requisition metadata.
 * @property storagePathPrefix Prefix path within the blob storage for grouped requisitions.
 * @property throttler Limits API call concurrency to prevent overload.
 * @property eventGroupsClient gRPC stub for retrieving event group details.
 * @property requisitionsClient gRPC stub for interacting with requisitions.
 */
class RequisitionGrouperByReportId(
  private val requisitionValidator: RequisitionsValidator,
  private val dataProviderName: String,
  private val blobUriPrefix: String,
  private val requisitionMetadataStub: RequisitionMetadataServiceCoroutineStub,
  private val storageClient: StorageClient,
  private val responsePageSize: Int,
  private val storagePathPrefix: String,
  throttler: Throttler,
  eventGroupsClient: EventGroupsCoroutineStub,
  requisitionsClient: RequisitionsCoroutineStub,
) : RequisitionGrouper(requisitionValidator, requisitionsClient, eventGroupsClient, throttler) {

  /**
   * Groups validated [Requisition]s by report ID and persists their metadata.
   *
   * ### High-Level Flow
   * 1. Partition requisitions by report ID.
   * 2. Fetch existing [RequisitionMetadata] entries for each report.
   * 3. Recover missing grouped requisitions from metadata when necessary.
   * 4. Create new grouped requisitions for unregistered requisitions and persist their metadata.
   *
   * @return A list of [GroupedRequisitions] for all processed reports.
   */
  override suspend fun createGroupedRequisitions(requisitions: List<Requisition>): List<GroupedRequisitions> {

    val groupedRequisitions = mutableListOf<GroupedRequisitions>()

    for ((reportId, requisitionsByReportId) in requisitions.groupBy { getReportId(it) }) {
      val requisitionsMetadata: List<RequisitionMetadata> = listRequisitionMetadataByReportId(reportId)
      val groupedRequisitionMetadata: Map<String, List<RequisitionMetadata>> = requisitionsMetadata.groupBy { it.groupId }
      groupedRequisitions.addAll(checkUnwrittenGroupedRequisitions(groupedRequisitionMetadata, requisitionsByReportId))
      createNewGroupedRequisitions(groupedRequisitionMetadata, requisitionsByReportId)
        ?.let { groupedRequisitions.add(it) }
    }

    return groupedRequisitions

  }

  /**
   * Validates a batch of requisitions and merges them into a single [GroupedRequisitions].
   *
   * ### High-Level Flow
   * 1. Validate each requisition’s [RequisitionSpec] and event group references.
   * 2. On validation failure, build a refusal and record it.
   * 3. On success, assemble a [GroupedRequisitions] with model line and event group map.
   * 4. Validate model-line consistency across requisitions using [RequisitionsValidator].
   * 5. Return the merged result and any refusals.
   *
   * @return A [GroupedRequisitionsWrapper] containing the merged result and refusal map.
   */
  suspend fun validateAndGroupRequisitions(
    requisitions: List<Requisition>,
    groupId: String
  ): GroupedRequisitionsWrapper {
    val groupedRequisitions = mutableListOf<GroupedRequisitions>()
    val refusals = mutableMapOf<Requisition, Refusal>()

    requisitions.forEach { requisition ->
      // Validate requisition spec
      val spec = try {
        requisitionValidator.validateRequisitionSpec(requisition)
      } catch (exception: Exception) {
        refusals[requisition] = buildRefusal(exception, requisition); return@forEach
      }
      // Get Event Group Map Entries
      val eventGroupMapEntries = try {
        getEventGroupMapEntries(spec)
      } catch (exception: Exception) {
        refusals[requisition] = buildRefusal(exception, requisition); return@forEach
      }
      // Create a single GroupedRequisition for the requisition
      val measurementSpec: MeasurementSpec = requisition.measurementSpec.unpack()
      groupedRequisitions += groupedRequisitions {
        modelLine = measurementSpec.modelLine
        this.requisitions += GroupedRequisitionsKt.requisitionEntry { this.requisition = Any.pack(requisition) }
        this.eventGroupMap +=
          eventGroupMapEntries.map {
            eventGroupMapEntry {
              this.eventGroup = it.key
              details = it.value
            }
          }
      }

    }

    if (groupedRequisitions.isEmpty()) return GroupedRequisitionsWrapper(null, refusals)

    return try {
      val reportId = getReportId(requisitions.first())
      requisitionValidator.validateModelLines(groupedRequisitions, reportId)
      val merged = mergeGroupedRequisitions(groupedRequisitions, groupId)
      GroupedRequisitionsWrapper(merged, refusals)
    } catch (e: InvalidRequisitionException) {
      GroupedRequisitionsWrapper(null, requisitions.associateWith { e.refusal })
    }

  }

  /**
   * Merges multiple [GroupedRequisitions] belonging to the same group into one.
   *
   * ### High-Level Flow
   * 1. Combine requisitions and event group maps for the given group.
   * 2. Merge overlapping collection intervals.
   * 3. Produce a single [GroupedRequisitions] with the shared `groupId`.
   */
  private fun mergeGroupedRequisitions(
    groupedRequisitions: List<GroupedRequisitions>,
    groupId: String
  ): GroupedRequisitions {
    val modelLine = groupedRequisitions.first().modelLine
    val mergedRequisitions = groupedRequisitions.flatMap { it.requisitionsList }
    val eventGroupMapEntries = buildEventGroupEntries(groupedRequisitions)

    return groupedRequisitions {
      this.modelLine = modelLine
      this.requisitions += mergedRequisitions
      this.eventGroupMap += eventGroupMapEntries
      this.groupId = groupId
    }

  }

  /**
   * Builds a refusal object for a failed requisition validation.
   *
   * Returns the refusal contained in [InvalidRequisitionException] if available,
   * otherwise builds a generic refusal with justification UNFULFILLABLE.
   */
  fun buildRefusal(e: Exception, requisition: Requisition): Refusal =
    when (e) {
      is InvalidRequisitionException -> e.refusal
      else -> refusal {
        justification = Requisition.Refusal.Justification.UNFULFILLABLE
        message = "Failed to process ${requisition.name}: ${e.message}"
      }
    }

  /**
   * Creates a new [GroupedRequisitions] for previously unregistered requisitions.
   *
   * ### High-Level Flow
   * 1. Identify requisitions not yet recorded in metadata.
   * 2. Validate and group them via [validateAndGroupRequisitions].
   * 3. Persist corresponding [RequisitionMetadata] entries through
   *    `requisitionMetadataStub.createRequisitionMetadata`.
   * 4. Refuse invalid requisitions both upstream (Kingdom) and in metadata storage.
   */
  suspend fun createNewGroupedRequisitions(
    groupedRequisitionMetadata: Map<String, List<RequisitionMetadata>>,
    requisitions: List<Requisition>
  ): GroupedRequisitions? {
    val existingCmmsRequisitionName: Set<String> = groupedRequisitionMetadata.values
      .flatten()
      .map { it.cmmsRequisition }
      .toSet()

    val unregisteredRequisitions: List<Requisition> = requisitions.filter {
      it.name !in existingCmmsRequisitionName
    }

    if (unregisteredRequisitions.isEmpty()) return null

    val requisitionGroupId = UUID.randomUUID().toString()
    val groupedRequisitionsWrapper = validateAndGroupRequisitions(unregisteredRequisitions, requisitionGroupId)
    groupedRequisitionsWrapper.refusals.forEach { (requisition, refusal) ->
      // Refuse to Kingdom first, then update Requisition Metadata Storage
      refuseRequisitionToKingdom(requisition, refusal)
    }

    return try {
      updateRequisitionMedatada(groupedRequisitionsWrapper,requisitionGroupId)
      groupedRequisitionsWrapper.groupedRequisitions
    } catch (exception: Exception){
      // If an exception occurs, some metadata may be written and the blob will be stored and the next invocation
      null
    }
  }

  /**
   * Updates metadata for successfully created or refused requisitions.
   *
   * ### High-Level Flow
   * 1. Create [RequisitionMetadata] entries for each successfully grouped requisition.
   * 2. For refused requisitions, call [createRequisitionMetadata] and [refuseRequisitionMetadata] to persist refusal state.
   */
  private suspend fun updateRequisitionMedatada(groupedRequisitionWrapper: GroupedRequisitionsWrapper, groupId: String) {
    groupedRequisitionWrapper.groupedRequisitions?.requisitionsList
      ?.map { it.requisition.unpack(Requisition::class.java) }
      ?.forEach { requisition ->
        createRequisitionMetadata(requisition, groupId)
      }

    groupedRequisitionWrapper.refusals.forEach { (requisition, refusal) ->
      val requisitionMetadata = createRequisitionMetadata(requisition, groupId)
      refuseRequisitionMetadata(requisitionMetadata, refusal.message)
    }
  }

  /**
   * Reconstructs missing grouped requisitions based on stored metadata.
   *
   * ### High-Level Flow
   * 1. For each metadata group, check if the corresponding blob exists in [StorageClient].
   * 2. If missing, re-validate and rebuild the grouped requisition via [validateAndGroupRequisitions].
   * 3. Return successfully rebuilt groups.
   */
  suspend fun checkUnwrittenGroupedRequisitions(
    groupedRequisitionMetadata: Map<String, List<RequisitionMetadata>>,
    requisitions: List<Requisition>
  ): List<GroupedRequisitions> =
    groupedRequisitionMetadata.mapNotNull { (groupId, requisitionsMetadataByGroupId) ->
      try {
        val blob = readGroupedRequisitionBlob(groupId)
        if (blob != null) return@mapNotNull null
        val filteredRequisitions = filterRequisitions(requisitions, requisitionsMetadataByGroupId)
        val groupedRequisitionsWrapper = validateAndGroupRequisitions(filteredRequisitions, groupId)
        groupedRequisitionsWrapper.groupedRequisitions ?: run {
          logger.info("Error while creating GroupedRequisitions for groupId=$groupId")
          null
        }
      } catch (exception: Exception){
        logger.info("Unable to create GroupedRequisition for existing Requisition Metadata for groupId: $groupId")
        return emptyList()
      }
    }

  /**
   * Filters requisitions to those that correspond to metadata records.
   *
   * Used to match existing metadata entries to current requisitions during recovery.
   */
  fun filterRequisitions(requisitions: List<Requisition>, requisitionsMetadata: List<RequisitionMetadata>): List<Requisition> {
    val metadataRequisitionNames = requisitionsMetadata.map { it.cmmsRequisition }.toSet()
    return requisitions.filter {
      it.name in metadataRequisitionNames
    }
  }


  /**
   * Builds event group map entries by merging overlapping collection intervals.
   *
   * Used internally when merging grouped requisitions belonging to the same report.
   */
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

  /**
   * Reads a [GroupedRequisitions] blob from [StorageClient] by its `groupId`.
   *
   * Returns `null` if the blob is missing.
   */
  private suspend fun readGroupedRequisitionBlob(groupId: String) : GroupedRequisitions? {
    val blobKey = "$storagePathPrefix/${groupId}"
    val blob = storageClient.getBlob(blobKey) ?: return null
    return Any.parseFrom(blob.read().flatten()).unpack(GroupedRequisitions::class.java)
  }

  /**
   * Lists [RequisitionMetadata] records for a specific report using the Requisition Metadata Storage.
   */
  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private suspend fun listRequisitionMetadataByReportId(reportName: String): List<RequisitionMetadata> {
    val requisitionMetadataList: Flow<RequisitionMetadata> =
      requisitionMetadataStub
        .listResources { pageToken: String ->
          val request = listRequisitionMetadataRequest {
            parent = dataProviderName
            filter = ListRequisitionMetadataRequestKt.filter { report = reportName }
            pageSize = responsePageSize
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

  /**
   * Creates and persists a [RequisitionMetadata] entry in the Requisition Metadata Storage.
   *
   * ### High-Level Flow
   * 1. Construct metadata with blob URI, group ID, and report reference.
   * 2. Persist it via [requisitionMetadataStub.createRequisitionMetadata].
   * 3. Return the created [RequisitionMetadata].
   */
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
    val createRequisitionMetadataRequestId = "$requisitionGroupId:${requisition.name}"
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
