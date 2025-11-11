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
import io.opentelemetry.api.trace.Span
import java.util.logging.Level
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
import org.wfanet.measurement.edpaggregator.telemetry.Tracing.traceSuspending
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
  private val metrics: RequisitionFetcherMetrics = RequisitionFetcherMetrics.Default,
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
  suspend fun fetchAndStoreRequisitions() {
    logger.info("Executing requisitionFetchingWorkflow for $dataProviderName...")

    val startMark = TimeSource.Monotonic.markNow()
    val requisitions = fetchRequisitions()
    val storedRequisitions: Int = storeRequisitions(requisitions)

    val latencySeconds = startMark.elapsedNow().inWholeMilliseconds / 1000.0
    recordFetchMetrics(latencySeconds)

    logger.info(
      "$storedRequisitions unfulfilled grouped requisitions have been persisted to storage for " +
        dataProviderName
    )
  }

  /**
   * Fetches unfulfilled requisitions from the Kingdom.
   *
   * @return the list of unfulfilled requisitions.
   * @throws Exception if there is an error while listing requisitions.
   */
  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private suspend fun fetchRequisitions(): List<Requisition> = withFetchTelemetry {
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
          ResourceList(response.requisitionsList, response.nextPageToken)
        }
        .flattenConcat()

    requisitions.toList()
  }

  /**
   * Groups and stores requisitions in persistent storage.
   *
   * @param requisitions Requisitions fetched from the Kingdom.
   * @return The number of grouped requisitions successfully stored.
   */
  private suspend fun storeRequisitions(requisitions: List<Requisition>): Int =
    withStoreTelemetry(requisitions.size) {
      val groupedRequisitions = requisitionGrouper.groupRequisitions(requisitions)
      val totalToStore = groupedRequisitions.count { it.requisitionsList.isNotEmpty() }
      var storedGroupedRequisitions = 0

      // Always return the grouped requisitions and counts so that withStoreTelemetry can emit
      // consistent metrics and logs before any exception is rethrown.
      try {
        groupedRequisitions.forEach { groupedRequisition: GroupedRequisitions ->
          if (groupedRequisition.requisitionsList.isNotEmpty()) {
            storeGroupedRequisition(groupedRequisition)
            storedGroupedRequisitions += 1
          }
        }

        StoreTelemetryResult(
          groupedRequisitions = groupedRequisitions,
          storedCount = storedGroupedRequisitions,
        )
      } catch (e: Exception) {
        val failedGroupedRequisitions = totalToStore - storedGroupedRequisitions
        // Attach failure metadata so the telemetry layer can log/record before surfacing the error.
        StoreTelemetryResult(
          groupedRequisitions = groupedRequisitions,
          storedCount = storedGroupedRequisitions,
          failedCount = failedGroupedRequisitions,
          exception = e,
        )
      }
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
    val requisitionCount = groupedRequisition.requisitionsList.size

    withStorageTelemetry(
      groupedRequisitionId = groupedRequisitionId,
      blobKey = blobKey,
      reportId = reportId,
      requisitionCount = requisitionCount,
    ) {
      storageClient.writeBlob(blobKey, Any.pack(groupedRequisition).toByteString())
    }
  }

  private suspend fun withFetchTelemetry(
    block: suspend () -> List<Requisition>
  ): List<Requisition> =
    traceSuspending(
      spanName = SPAN_FETCH_REQUISITIONS,
      attributes = Attributes.of(ATTR_DATA_PROVIDER_KEY, dataProviderName),
    ) {
      try {
        val requisitions = block()
        val requisitionsCount = requisitions.size

        metrics.requisitionsFetched.add(
          requisitionsCount.toLong(),
          Attributes.of(ATTR_DATA_PROVIDER_KEY, dataProviderName),
        )

        if (requisitionsCount > 0) {
          Span.current()
            .addEvent(
              EVENT_FETCH_COMPLETED,
              Attributes.of(
                ATTR_DATA_PROVIDER_KEY,
                dataProviderName,
                ATTR_REQUISITION_COUNT_KEY,
                requisitionsCount.toLong(),
              ),
            )
        }

        logger.info("Fetched $requisitionsCount requisitions for $dataProviderName")

        requisitions
      } catch (e: Exception) {
        Span.current()
          .addEvent(
            EVENT_FETCH_FAILED,
            Attributes.of(
              ATTR_DATA_PROVIDER_KEY,
              dataProviderName,
              ATTR_STATUS_KEY,
              STATUS_FAILURE,
              ATTR_ERROR_TYPE_KEY,
              e::class.simpleName ?: e::class.java.simpleName ?: e::class.java.name,
            ),
          )
        throw e
      }
    }

  private data class StoreTelemetryResult(
    val groupedRequisitions: List<GroupedRequisitions>,
    val storedCount: Int,
    val failedCount: Int = 0,
    val exception: Exception? = null,
  )

  private suspend fun withStoreTelemetry(
    requisitionCount: Int,
    block: suspend () -> StoreTelemetryResult,
  ): Int =
    traceSuspending(
      spanName = SPAN_STORE_REQUISITIONS,
      attributes = Attributes.of(ATTR_DATA_PROVIDER_KEY, dataProviderName),
    ) {
      val result =
        try {
          block()
        } catch (e: Exception) {
          Span.current()
            .addEvent(
              EVENT_STORE_FAILED,
              Attributes.of(
                ATTR_DATA_PROVIDER_KEY,
                dataProviderName,
                ATTR_STATUS_KEY,
                STATUS_FAILURE,
                ATTR_ERROR_TYPE_KEY,
                e::class.simpleName ?: e::class.java.simpleName ?: e::class.java.name,
              ),
            )
          throw e
        }

      if (result.exception == null) {
        Span.current()
          .addEvent(
            EVENT_STORE_COMPLETED,
            Attributes.of(
              ATTR_DATA_PROVIDER_KEY,
              dataProviderName,
              ATTR_REQUISITION_COUNT_KEY,
              requisitionCount.toLong(),
              ATTR_GROUPED_COUNT_KEY,
              result.groupedRequisitions.size.toLong(),
              ATTR_STORAGE_COUNT_KEY,
              result.storedCount.toLong(),
              ATTR_STATUS_KEY,
              STATUS_SUCCESS,
            ),
          )
        logger.info(
          "Storage summary: ${result.storedCount} succeeded, 0 failed for $dataProviderName"
        )
        return@traceSuspending result.storedCount
      }

      val exception = result.exception!!
      Span.current()
        .addEvent(
          EVENT_STORE_FAILED,
          Attributes.of(
            ATTR_DATA_PROVIDER_KEY,
            dataProviderName,
            ATTR_REQUISITION_COUNT_KEY,
            requisitionCount.toLong(),
            ATTR_GROUPED_COUNT_KEY,
            result.groupedRequisitions.size.toLong(),
            ATTR_STORAGE_COUNT_KEY,
            result.storedCount.toLong(),
            ATTR_STATUS_KEY,
            STATUS_FAILURE,
            ATTR_ERROR_TYPE_KEY,
            exception::class.simpleName
              ?: exception::class.java.simpleName
              ?: exception::class.java.name,
          ),
        )
      if (result.failedCount > 0) {
        metrics.storageFails.add(
          result.failedCount.toLong(),
          Attributes.of(ATTR_DATA_PROVIDER_KEY, dataProviderName),
        )
        logger.severe(
          "Storage summary: ${result.storedCount} succeeded, " +
            "${result.failedCount} failed for $dataProviderName"
        )
      }
      throw exception
    }

  private suspend fun withStorageTelemetry(
    groupedRequisitionId: String,
    blobKey: String,
    reportId: String?,
    requisitionCount: Int,
    block: suspend () -> Unit,
  ) {
    val spanAttributes =
      Attributes.builder()
        .put(ATTR_GROUPED_REQUISITION_ID_KEY, groupedRequisitionId)
        .put(ATTR_DATA_PROVIDER_KEY, dataProviderName)
        .apply { if (reportId != null) put(ATTR_REPORT_ID_KEY, reportId) }
        .build()

    traceSuspending(spanName = SPAN_STORE_GROUPED_REQUISITION, attributes = spanAttributes) {
      logger.info(
        "Storing $requisitionCount requisitions: $blobKey for $dataProviderName " +
          "(groupedRequisitionId=$groupedRequisitionId)"
      )

      try {
        block()

        metrics.storageWrites.add(1, Attributes.of(ATTR_DATA_PROVIDER_KEY, dataProviderName))
        Span.current()
          .addEvent(
            EVENT_STORAGE_WRITE,
            Attributes.of(
              ATTR_DATA_PROVIDER_KEY,
              dataProviderName,
              ATTR_GROUPED_REQUISITION_ID_KEY,
              groupedRequisitionId,
              ATTR_REPORT_ID_KEY,
              reportId ?: UNKNOWN_REPORT_ID,
              ATTR_REQUISITION_COUNT_KEY,
              requisitionCount.toLong(),
              ATTR_STATUS_KEY,
              STATUS_SUCCESS,
            ),
          )
      } catch (e: Exception) {
        Span.current()
          .addEvent(
            EVENT_STORAGE_WRITE_FAILED,
            Attributes.of(
              ATTR_DATA_PROVIDER_KEY,
              dataProviderName,
              ATTR_GROUPED_REQUISITION_ID_KEY,
              groupedRequisitionId,
              ATTR_REPORT_ID_KEY,
              reportId ?: UNKNOWN_REPORT_ID,
              ATTR_STATUS_KEY,
              STATUS_FAILURE,
              ATTR_ERROR_TYPE_KEY,
              e::class.simpleName ?: e::class.java.simpleName ?: e::class.java.name,
            ),
          )
        throw e
      }
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
      logger.log(Level.WARNING, "Failed to extract report_id for $dataProviderName", e)
      null
    }
  }

  // Telemetry helper functions

  private fun recordFetchMetrics(latencySeconds: Double) {
    metrics.fetchLatency.record(
      latencySeconds,
      Attributes.of(ATTR_DATA_PROVIDER_KEY, dataProviderName),
    )
  }

  companion object {
    private val logger: Logger = Logger.getLogger(RequisitionFetcher::class.java.name)
    private val ATTR_DATA_PROVIDER_KEY = AttributeKey.stringKey("edpa.data_provider_name")
    private val ATTR_REQUISITION_COUNT_KEY =
      AttributeKey.longKey("edpa.requisition_fetcher.requisition_count")
    private val ATTR_GROUPED_COUNT_KEY =
      AttributeKey.longKey("edpa.requisition_fetcher.grouped_requisition_count")
    private val ATTR_STORAGE_COUNT_KEY =
      AttributeKey.longKey("edpa.requisition_fetcher.storage_success_count")
    private val ATTR_GROUPED_REQUISITION_ID_KEY =
      AttributeKey.stringKey("edpa.grouped_requisition_id")
    private val ATTR_REPORT_ID_KEY = AttributeKey.stringKey("edpa.report_id")
    private val ATTR_STATUS_KEY = AttributeKey.stringKey("edpa.requisition_fetcher.status")
    private val ATTR_ERROR_TYPE_KEY = AttributeKey.stringKey("edpa.requisition_fetcher.error_type")

    private const val STATUS_SUCCESS = "success"
    private const val STATUS_FAILURE = "failure"

    private const val SPAN_FETCH_REQUISITIONS = "edpa.requisition_fetcher.requisition_fetch"
    private const val SPAN_STORE_REQUISITIONS = "edpa.requisition_fetcher.requisition_store"
    private const val SPAN_STORE_GROUPED_REQUISITION =
      "edpa.requisition_fetcher.store_grouped_requisition"

    private const val EVENT_FETCH_COMPLETED = "requisition_fetch_completed"
    private const val EVENT_FETCH_FAILED = "requisition_fetch_failed"
    private const val EVENT_STORE_COMPLETED = "requisition_store_completed"
    private const val EVENT_STORE_FAILED = "requisition_store_failed"
    private const val EVENT_STORAGE_WRITE = "requisition_fetcher.storage_write"
    private const val EVENT_STORAGE_WRITE_FAILED = "requisition_fetcher.storage_write_failed"

    private const val UNKNOWN_REPORT_ID = "unknown"
  }
}
