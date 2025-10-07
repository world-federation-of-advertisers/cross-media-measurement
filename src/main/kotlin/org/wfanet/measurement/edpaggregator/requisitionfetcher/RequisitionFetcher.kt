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
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import java.util.logging.Logger
import kotlin.time.TimeSource
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2alpha.ListRequisitionsRequestKt
import org.wfanet.measurement.api.v2alpha.ListRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.listRequisitionsRequest
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.flattenConcat
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.edpaggregator.telemetry.TracedOperation
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.storage.StorageClient

/**
 * Fetches requisitions from the Kingdom and persists them into GCS.
 *
 * @param requisitionsStub used to pull [Requisition]s from the kingdom
 * @param storageClient client used to store [Requisition]s
 * @param dataProviderName of the EDP for which [Requisition]s will be retrieved
 * @param storagePathPrefix the blob key prefix to use when storing a [Requisition]
 * @param requisitionGrouper the instance of [RequisitionGrouper] to use to group requisitions
 * @param responsePageSize
 * @param metrics OpenTelemetry metrics instance for tracking operations
 */
class RequisitionFetcher(
  private val requisitionsStub: RequisitionsCoroutineStub,
  private val storageClient: StorageClient,
  private val dataProviderName: String,
  private val storagePathPrefix: String,
  private val requisitionGrouper: RequisitionGrouper,
  private val responsePageSize: Int? = null,
  private val metrics: RequisitionFetcherMetrics = RequisitionFetcherMetrics.instance,
) {

  /**
   * Fetches and stores unfulfilled requisitions from a data provider.
   *
   * This method executes a workflow to retrieve requisitions that are in the "UNFULFILLED" state
   * from a specified data provider and stores them in persistent storage. It handles pagination by
   * using a `pageToken` to fetch all available pages of requisitions.
   *
   * @throws Exception if there is an error while listing requisitions.
   */
  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  suspend fun fetchAndStoreRequisitions() = TracedOperation.trace(
    spanName = "requisition_fetch_and_store",
    attributes = mapOf("data_provider" to dataProviderName)
  ) {
    logger.info("Executing requisitionFetchingWorkflow for $dataProviderName...")

    val startMark = TimeSource.Monotonic.markNow()
    val requisitions = fetchRequisitions()

    logger.info("Fetched ${requisitions.size} requisitions...")
    val groupedRequisitions: List<GroupedRequisitions> =
      requisitionGrouper.groupRequisitions(requisitions)
    logger.info("GroupedRequisitions: $groupedRequisitions")
    val storedRequisitions: Int = storeRequisitions(groupedRequisitions)

    // Emit fetch latency metric (full operation: fetch + group + store)
    val latencySeconds = startMark.elapsedNow().inWholeMilliseconds / 1000.0
    metrics.fetchLatency.record(
      latencySeconds,
      Attributes.builder()
        .put(DATA_PROVIDER_NAME, dataProviderName)
        .build()
    )

    logger.info {
      "$storedRequisitions unfulfilled grouped requisitions have been persisted to storage for $dataProviderName"
    }
  } // End TracedOperation.trace

  /**
   * Fetches unfulfilled requisitions from the Kingdom.
   *
   * @return the list of unfulfilled requisitions.
   * @throws Exception if there is an error while listing requisitions.
   */
  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private suspend fun fetchRequisitions(): List<Requisition> {
    var requisitionsCount = 0

    val requisitions: Flow<Requisition> =
      requisitionsStub
        .listResources { pageToken: String ->
          val request = listRequisitionsRequest {
            parent = dataProviderName
            filter = ListRequisitionsRequestKt.filter { states += Requisition.State.UNFULFILLED }
            if (responsePageSize != null) {
              pageSize = responsePageSize
            }
            this.pageToken = pageToken
          }
          val response: ListRequisitionsResponse =
            try {
              requisitionsStub.listRequisitions(request)
            } catch (e: StatusException) {
              throw Exception("Error listing requisitions", e)
            }
          requisitionsCount += response.requisitionsList.size
          ResourceList(response.requisitionsList, response.nextPageToken)
        }
        .flattenConcat()

    val requisitionsList = requisitions.toList()

    // Emit requisitions fetched counter
    metrics.requisitionsFetched.add(
      requisitionsCount.toLong(),
      Attributes.builder()
        .put(DATA_PROVIDER_NAME, dataProviderName)
        .build()
    )

    return requisitionsList
  }

  /**
   * Stores a list of grouped requisitions in persistent storage.
   *
   * @param groupedRequisitions A list of grouped requisitions to be stored.
   * @return The number of grouped requisitions successfully stored.
   */
  private suspend fun storeRequisitions(groupedRequisitions: List<GroupedRequisitions>): Int {
    var storedGroupedRequisitions = 0
    val totalRequisitions = groupedRequisitions.count { it.requisitionsList.isNotEmpty() }

    try {
      groupedRequisitions.forEach { groupedRequisition: GroupedRequisitions ->
        if (groupedRequisition.requisitionsList.isNotEmpty()) {
          storeGroupedRequisition(groupedRequisition)
          storedGroupedRequisitions += 1
        }
      }
    } catch (e: Exception) {
      val failedGroupedRequisitions = totalRequisitions - storedGroupedRequisitions

      // Log summary
      logger.severe("Storage summary: $storedGroupedRequisitions succeeded, $failedGroupedRequisitions failed")

      // Emit storage failure metric
      metrics.storageFails.add(
        failedGroupedRequisitions.toLong(),
        Attributes.builder()
          .put(DATA_PROVIDER_NAME, dataProviderName)
          .build()
      )

      // Re-raise the exception
      throw e
    }

    logger.info("Storage summary: $storedGroupedRequisitions succeeded, 0 failed")
    return storedGroupedRequisitions
  }

  /**
   * Stores a single grouped requisition to persistent storage with tracing and metrics.
   *
   * @param groupedRequisition The grouped requisition to store.
   * @throws Exception if storage fails.
   */
  private suspend fun storeGroupedRequisition(groupedRequisition: GroupedRequisitions) {
    val groupedRequisitionId = groupedRequisition.groupId
    val blobKey = "$storagePathPrefix/${groupedRequisitionId}"
    val reportId = extractReportId(groupedRequisition)

    // Create span attributes with report_id for tracing
    val spanAttributes = mutableMapOf(
      "grouped_requisition_id" to groupedRequisitionId,
      "data_provider" to dataProviderName
    )
    if (reportId != null) {
      spanAttributes["report_id"] = reportId
    }

    TracedOperation.trace(
      spanName = "store_grouped_requisition",
      attributes = spanAttributes
    ) {
      logger.info("Storing ${groupedRequisition.requisitionsList.size} requisitions: $blobKey")
      storageClient.writeBlob(blobKey, Any.pack(groupedRequisition).toByteString())

      // Emit storage write metric
      metrics.storageWrites.add(
        1,
        Attributes.builder()
          .put(DATA_PROVIDER_NAME, dataProviderName)
          .build()
      )
    }
  }

  /**
   * Extracts the report_id from a GroupedRequisitions object.
   *
   * @param groupedRequisitions The grouped requisitions to extract report_id from
   * @return The report_id string, or null if not found
   */
  private fun extractReportId(groupedRequisitions: GroupedRequisitions): String? {
    return try {
      if (groupedRequisitions.requisitionsList.isNotEmpty()) {
        val firstEntry = groupedRequisitions.requisitionsList.first()
        val requisition = firstEntry.requisition.unpack(Requisition::class.java)
        val measurementSpec: MeasurementSpec = requisition.measurementSpec.unpack()
        measurementSpec.reportingMetadata.report
      } else {
        null
      }
    } catch (e: Exception) {
      logger.warning("Failed to extract report_id: ${e.message}")
      null
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private val DATA_PROVIDER_NAME = AttributeKey.stringKey("data_provider_name")
  }
}
