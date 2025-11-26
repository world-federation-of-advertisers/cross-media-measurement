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
import java.util.UUID
import java.util.logging.Logger
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.Requisition.Refusal
import org.wfanet.measurement.api.v2alpha.RequisitionKt.refusal
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.SignedMessage
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
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions.EventGroupMapEntry
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt
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
import org.wfanet.measurement.edpaggregator.v1alpha.requisitionMetadata
import org.wfanet.measurement.storage.StorageClient

/**
 * The result of a report validation.
 *
 * If a refusal is present, then the entire list of requisitions will be refused with the same
 * refusal reason.
 */
private data class ReportValidationOutcome(
  val requisitions: List<Requisition>,
  val refusal: Refusal?,
)

/**
 * Groups [Requisition]s by **Report ID** and manages their lifecycle in the Requisition Metadata
 * Storage.
 *
 * This class aggregates all [Requisition]s associated with the same report into a single
 * [GroupedRequisitions] message. It ensures each group is validated, persisted, and recoverable
 * through its metadata records. Requisitions failing validation are refused both to Cmms and to the
 * Requisition Metadata Storage.
 *
 * ### High-Level Flow
 * 1. **Group by Report ID** — Partition requisitions by their report field in
 *    [MeasurementSpec.reportingMetadata.report].
 * 2. **List Existing Metadata** — Retrieve existing [RequisitionMetadata] entries for the report
 *    using the Requisition Metadata Storage (`requisitionMetadataStub.listRequisitionMetadata`).
 * 3. **Recover Missing Groups** — For metadata entries whose blobs are missing in [StorageClient],
 *    re-create the grouped requisitions.
 * 4. **Validate New Requisitions** — Validate unregistered requisitions. On failure, record
 *    refusals to Cmms and in metadata storage.
 * 5. **Persist Metadata for New Groups** — For valid requisitions, create new [RequisitionMetadata]
 *    entries via `requisitionMetadataStub.createRequisitionMetadata`, associating each requisition
 *    with a generated group ID and blob URI.
 *
 * @property requisitionValidator Validates that all grouped requisitions are consistent and
 *   compatible.
 * @property dataProviderName The name of the data provider resource, used as the parent in metadata
 *   requests.
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
  override suspend fun createGroupedRequisitions(
    requisitions: List<Requisition>
  ): List<GroupedRequisitions> {
    val groupedRequisitions = mutableListOf<GroupedRequisitions>()
    for ((reportId, requisitionsByReportId) in requisitions.groupBy { getReportId(it) }) {
      val requisitionsMetadata: List<RequisitionMetadata> =
        listRequisitionMetadataByReportId(reportId)
      val storedRequisitionMetadata =
        requisitionsMetadata.filter { it.state == RequisitionMetadata.State.STORED }
      val storedGroupIdToRequisitionMetadata: Map<String, List<RequisitionMetadata>> =
        storedRequisitionMetadata.groupBy { it.groupId }
      groupedRequisitions.addAll(
        recoverUnpersistedGroupedRequisitions(
          storedGroupIdToRequisitionMetadata,
          requisitionsByReportId,
        )
      )
      val groupIdToRequisitionMetadata: Map<String, List<RequisitionMetadata>> =
        requisitionsMetadata.groupBy { it.groupId }
      getNewGroupedRequisitions(groupIdToRequisitionMetadata, reportId, requisitionsByReportId)
        ?.let { groupedRequisitions.add(it) }
    }
    return groupedRequisitions
  }

  /**
   * Validates requisitions. If any requisition is invalid, all requisitions with that reportId are
   * marked as invalid, as well.
   *
   * @param reportId The unique identifier of the report whose requisitions are being validated.
   * @param requisitions The list of all [Requisition] objects associated with the report.
   * @return A [ReportValidationOutcome] indicating whether the requisitions are valid or refused.
   */
  private fun validateRequisitionsByReport(
    reportId: String,
    requisitions: List<Requisition>,
  ): ReportValidationOutcome {
    // Check for invalid requisition spec
    requisitions.forEach { requisition ->
      try {
        requisitionValidator.validateRequisitionSpec(requisition)
      } catch (e: InvalidRequisitionException) {
        return ReportValidationOutcome(requisitions = requisitions, refusal = e.refusal)
      }
    }
    // Check for inconsistent model lines
    val modelLine = getModelLine(requisitions.first().measurementSpec)
    val invalidModelLines =
      requisitions.any { requisition -> modelLine != getModelLine(requisition.measurementSpec) }
    if (invalidModelLines) {
      return ReportValidationOutcome(
        requisitions = requisitions,
        refusal =
          refusal {
            justification = Requisition.Refusal.Justification.UNFULFILLABLE
            message = "Report $reportId cannot contain multiple model lines"
          },
      )
    }
    return ReportValidationOutcome(requisitions, null)
  }

  private fun getModelLine(measurementSpec: SignedMessage): String {
    return measurementSpec.unpack<MeasurementSpec>().modelLine
  }

  /**
   * Groups a non-empty list of valid [Requisition]s into a single [GroupedRequisitions] object.
   *
   * ### High-Level Flow
   * 1. Unpacks each [Requisition]'s [MeasurementSpec].
   * 2. Extracts event group mappings from the [RequisitionSpec].
   * 3. Builds intermediate [GroupedRequisitions] entries for each requisition.
   * 4. Merges all grouped entries into a single [GroupedRequisitions] identified by the provided
   *    [groupId].
   *
   * If the [requisitions] list is empty, the function returns `null`.
   *
   * @param requisitions The list of validated [Requisition]s to group.
   * @param groupId The unique identifier to assign to the resulting [GroupedRequisitions].
   * @return A merged [GroupedRequisitions] containing all valid requisitions, or `null` if no
   *   requisitions were provided.
   */
  suspend private fun groupValidRequisitions(
    requisitions: List<Requisition>,
    groupId: String,
  ): GroupedRequisitions? {
    if (requisitions.isEmpty()) return null
    val requisitionsGroupedByReportId =
      requisitions.map { req ->
        val measurementSpec: MeasurementSpec = req.measurementSpec.unpack()
        val eventGroupMapEntries =
          getEventGroupMapEntries(requisitionValidator.getRequisitionSpec(req))
        groupedRequisitions {
          modelLine = measurementSpec.modelLine
          this.requisitions +=
            GroupedRequisitionsKt.requisitionEntry { requisition = Any.pack(req) }
          this.eventGroupMap +=
            eventGroupMapEntries.map {
              eventGroupMapEntry {
                eventGroup = it.key
                details = it.value
              }
            }
        }
      }
    return mergeGroupedRequisitions(requisitionsGroupedByReportId, groupId)
  }

  /**
   * Merges multiple [GroupedRequisitions] belonging to the same group into one.
   *
   * This function assumes that the provided [groupedRequisitions] list has been **pre-validated** —
   * meaning all entries belong to the same group and have consistent [modelLine] and related
   * metadata.
   *
   * ### High-Level Flow
   * 1. Combine requisitions and event group maps for the given group.
   * 2. Merge overlapping collection intervals.
   * 3. Produce a single [GroupedRequisitions] with the shared `groupId`.
   *
   * @param groupedRequisitions The list of [GroupedRequisitions] objects to merge. All entries must
   *   belong to the same group.
   * @param groupId The identifier assigned to the merged [GroupedRequisitions].
   * @return A single [GroupedRequisitions] containing all requisitions and event group mappings
   *   combined under the specified [groupId].
   */
  private fun mergeGroupedRequisitions(
    groupedRequisitions: List<GroupedRequisitions>,
    groupId: String,
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
   * Creates a new [GroupedRequisitions] for **new or unseen requisitions** not yet recorded in
   * metadata.
   *
   * ### High-Level Flow
   * 1. Identify requisitions missing from existing [RequisitionMetadata] entries.
   * 2. Validate the new requisitions via [validateRequisitionsByReport].
   * 3. If all requisitions are valid, group them using [groupValidRequisitions].
   * 4. If any requisition is invalid, refuse all of them to the Cmms.
   * 5. Update [RequisitionMetadata] to reflect the outcome of processing, persisting newly grouped
   *    requisitions and marking refused ones accordingly via [syncRequisitionMetadata].
   *
   * @param groupIdToRequisitionMetadata A map of requisition group ID to the list of
   *   [RequisitionMetadata] entries that belong to that group within a single report.
   * @param requisitions The list of all [Requisition] objects for a single report,
   * * potentially spanning multiple groups. This includes both previously seen and new
   *   requisitions.
   *
   * @return A newly created [GroupedRequisitions] if all requisitions are valid, or `null`
   *   otherwise.
   */
  suspend fun getNewGroupedRequisitions(
    groupIdToRequisitionMetadata: Map<String, List<RequisitionMetadata>>,
    reportId: String,
    requisitionsByReportId: List<Requisition>,
  ): GroupedRequisitions? {
    val existingCmmsRequisitionName: Set<String> =
      groupIdToRequisitionMetadata.values.flatten().map { it.cmmsRequisition }.toSet()
    val unregisteredRequisitionsByReportId: List<Requisition> =
      requisitionsByReportId.filter { it.name !in existingCmmsRequisitionName }
    if (unregisteredRequisitionsByReportId.isEmpty()) return null
    val requisitionGroupId = UUID.randomUUID().toString()
    val reportValidationOutcome =
      validateRequisitionsByReport(reportId, unregisteredRequisitionsByReportId)
    val groupedRequisitions =
      if (reportValidationOutcome.refusal != null) {
        reportValidationOutcome.requisitions.forEach { requisition ->
          // Refuse to Cmms first, then update Requisition Metadata Storage
          refuseRequisitionToCmms(requisition, reportValidationOutcome.refusal)
        }
        null
      } else {
        groupValidRequisitions(reportValidationOutcome.requisitions, requisitionGroupId)
      }
    syncRequisitionMetadata(reportValidationOutcome, requisitionGroupId)
    return groupedRequisitions
  }

  /**
   * Create metadata for successfully created and refuse the invalid ones.
   *
   * ### High-Level Flow
   * 1. Create [RequisitionMetadata] entries for each successfully grouped requisition.
   * 1. Create and Refuse [RequisitionMetadata] entries for each requisition that failed validation.
   */
  private suspend fun syncRequisitionMetadata(
    validationOutcome: ReportValidationOutcome,
    groupId: String,
  ) {
    if (validationOutcome.refusal == null) {
      validationOutcome.requisitions.forEach { requisition ->
        createRequisitionMetadata(requisition, groupId)
      }
    } else {
      validationOutcome.requisitions.forEach { requisition ->
        val requisitionMetadata = createRequisitionMetadata(requisition, groupId)
        val refusal = validationOutcome.refusal
        refuseRequisitionMetadata(requisitionMetadata, refusal.message)
      }
    }
  }

  /**
   * Reconstructs missing grouped requisitions based on stored metadata.
   *
   * ### High-Level Flow
   * 1. For each metadata group, check if the corresponding blob exists in [StorageClient].
   * 2. If missing, rebuild the grouped requisition via [validateAndGroupRequisitions].
   * 3. Return successfully rebuilt groups.
   *
   * This method does **not** perform validation on the provided [Requisition] objects, as they were
   * already validated during a previous run of the Cloud Function.
   *
   * @param groupIdToRequisitionMetadata All [RequisitionMetadata] entries for a single report,
   *   organized by group ID.
   * @param requisitions The list of all [Requisition] objects for the same report, potentially
   *   spanning multiple groups.
   */
  private suspend fun recoverUnpersistedGroupedRequisitions(
    groupIdToRequisitionMetadata: Map<String, List<RequisitionMetadata>>,
    requisitions: List<Requisition>,
  ): List<GroupedRequisitions> =
    groupIdToRequisitionMetadata.mapNotNull { (groupId, requisitionsMetadata) ->
      val blob = readGroupedRequisitionBlob(groupId)
      if (blob != null) return@mapNotNull null
      val filteredRequisitions = hasMetadata(requisitions, requisitionsMetadata)
      val groupedRequisitions = groupValidRequisitions(filteredRequisitions, groupId)
      groupedRequisitions
    }

  /**
   * Filters requisitions to those that correspond to metadata records.
   *
   * Used to match existing metadata entries to current requisitions during recovery.
   */
  private fun hasMetadata(
    requisitions: List<Requisition>,
    requisitionsMetadata: List<RequisitionMetadata>,
  ): List<Requisition> {
    val metadataRequisitionNames = requisitionsMetadata.map { it.cmmsRequisition }.toSet()
    return requisitions.filter { it.name in metadataRequisitionNames }
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
        val merged = unionIntervals(intervals)
        eventGroupMapEntry {
          eventGroup = eventGroupName
          details = eventGroupDetails {
            eventGroupReferenceId = refId
            collectionIntervals += merged
          }
        }
      }

  /**
   * Merges overlapping or contiguous time intervals into a minimal set of non-overlapping
   * intervals.
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
  private suspend fun readGroupedRequisitionBlob(groupId: String): GroupedRequisitions? {
    val blobKey = "$storagePathPrefix/${groupId}"
    val blob = storageClient.getBlob(blobKey) ?: return null
    return Any.parseFrom(blob.read().flatten()).unpack(GroupedRequisitions::class.java)
  }

  /**
   * Lists [RequisitionMetadata] records for the given report.
   *
   * This fetches all requisition metadata entries for the report and filters them client-side to
   * include only those in the `STORED`, `QUEUED`, or `PROCESSING` states.
   */
  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private suspend fun listRequisitionMetadataByReportId(
    reportName: String
  ): List<RequisitionMetadata> {
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
            requisitionMetadataStub.listRequisitionMetadata(request)
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
    val createRequisitionMetadataRequestId = UUID.randomUUID().toString()
    val request = createRequisitionMetadataRequest {
      parent = dataProviderName
      requisitionMetadata = metadata
      requestId = createRequisitionMetadataRequestId
    }
    return requisitionMetadataStub.createRequisitionMetadata(request)
  }

  /**
   * Marks the given [RequisitionMetadata] as refused and updates its status in metadata storage.
   *
   * This function builds and sends a `RefuseRequisitionMetadataRequest` using the provided
   * [requisitionMetadata] and refusal [message]. The operation is executed asynchronously via
   * [requisitionMetadataStub].
   *
   * @param requisitionMetadata The [RequisitionMetadata] entry to be marked as refused.
   * @param message The reason or explanatory message for refusing the requisition.
   */
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
