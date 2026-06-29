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
import com.google.protobuf.Timestamp
import io.grpc.StatusException
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.Span
import java.util.UUID
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.time.TimeSource
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import org.wfanet.measurement.api.v2alpha.ListRequisitionsRequestKt
import org.wfanet.measurement.api.v2alpha.ListRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionKt.refusal
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.listRequisitionsRequest
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.flattenConcat
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.edpaggregator.telemetry.Tracing.traceSuspending
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.ListRequisitionMetadataRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListRequisitionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.createRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.refuseRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.requisitionMetadata
import org.wfanet.measurement.storage.StorageClient

/**
 * Fetches requisitions from the Kingdom and persists them, grouped by report, to a [StorageClient].
 *
 * The fetcher streams requisitions through a bounded channel into a pool of per-report workers.
 * Each worker:
 * 1. Lists existing [RequisitionMetadata] for its report.
 * 2. Recovers any STORED metadata whose blob is missing by re-running the grouper.
 * 3. For the requisitions that have no metadata yet, validates them, writes the grouped blob first,
 *    and only then creates `STORED` metadata for each requisition.
 *
 * Writing the blob before any metadata is the wedge fix: a crash between blob write and metadata
 * create leaves a recoverable state (no metadata, next run re-groups); after metadata is created
 * the invariant `STORED metadata implies blob present` holds.
 *
 * @property requisitionsStub used to stream [Requisition]s from the Kingdom.
 * @property requisitionMetadataStub used for all Requisition Metadata Service RPCs.
 * @property storageClient used to write grouped requisition blobs.
 * @property dataProviderName resource name of the data provider being fetched for.
 * @property storagePathPrefix prefix prepended to each blob key.
 * @property blobUriPrefix prefix prepended to each metadata blob URI.
 * @property requisitionValidator validates per-report requisitions before grouping.
 * @property requisitionGrouper the in-memory grouper that builds [GroupedRequisitions].
 * @property metadataThrottler throttles all Requisition Metadata Service RPCs.
 * @property responsePageSize optional page size for `listRequisitions`.
 * @property metadataPageSize page size for `listRequisitionMetadata`.
 * @property maxBufferedRequisitionsPerReport upper bound on the number of requisitions buffered for
 *   a single `(reportId, updateTime)` tuple before the buffer is dispatched and a new one is
 *   started.
 * @property workerCount number of concurrent per-report workers.
 * @property channelCapacity capacity of the channel between the stream producer and workers.
 * @property metrics OpenTelemetry metrics sink.
 */
class RequisitionFetcher(
  private val requisitionsStub: RequisitionsCoroutineStub,
  private val requisitionMetadataStub: RequisitionMetadataServiceCoroutineStub,
  private val storageClient: StorageClient,
  private val dataProviderName: String,
  private val storagePathPrefix: String,
  private val blobUriPrefix: String,
  private val requisitionValidator: RequisitionsValidator,
  private val requisitionGrouper: RequisitionGrouperByReportId,
  private val metadataThrottler: Throttler,
  private val responsePageSize: Int? = null,
  private val metadataPageSize: Int = DEFAULT_METADATA_PAGE_SIZE,
  private val maxBufferedRequisitionsPerReport: Int = DEFAULT_MAX_BUFFERED_REQUISITIONS_PER_REPORT,
  private val workerCount: Int = DEFAULT_WORKER_COUNT,
  private val channelCapacity: Int = DEFAULT_CHANNEL_CAPACITY,
  private val metrics: RequisitionFetcherMetrics = RequisitionFetcherMetrics.Default,
) {

  private data class ReportWorkUnit(val reportId: String, val requisitions: List<Requisition>)

  private class OpenBuffer(
    val reportId: String,
    var currentUpdateTime: Timestamp,
    val requisitions: MutableList<Requisition> = mutableListOf(),
  )

  /** Streams unfulfilled requisitions for [dataProviderName] and persists them to storage. */
  suspend fun fetchAndStoreRequisitions() {
    logger.info("Executing requisitionFetchingWorkflow for $dataProviderName...")
    val startMark = TimeSource.Monotonic.markNow()

    val totalFetched = streamAndProcess()

    val latencySeconds = startMark.elapsedNow().inWholeMilliseconds / 1000.0
    metrics.fetchLatency.record(latencySeconds, dataProviderAttrs)

    logger.info("Fetched $totalFetched requisitions for $dataProviderName")
  }

  private suspend fun streamAndProcess(): Long =
    traceSuspending(spanName = SPAN_FETCH_REQUISITIONS, attributes = dataProviderAttrs) {
      var totalFetched = 0L
      coroutineScope {
        val channel = Channel<ReportWorkUnit>(channelCapacity)
        val workers =
          (0 until workerCount).map {
            launch {
              for (unit in channel) {
                processReport(unit)
              }
            }
          }

        try {
          totalFetched = produceWorkUnits(channel)
        } finally {
          channel.close()
        }
        workers.forEach { it.join() }
      }
      metrics.requisitionsFetched.add(totalFetched, dataProviderAttrs)
      Span.current()
        .addEvent(
          EVENT_FETCH_COMPLETED,
          Attributes.builder()
            .put(ATTR_DATA_PROVIDER_KEY, dataProviderName)
            .put(ATTR_REQUISITION_COUNT_KEY, totalFetched)
            .build(),
        )
      totalFetched
    }

  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private suspend fun produceWorkUnits(channel: Channel<ReportWorkUnit>): Long {
    val flow: Flow<Requisition> =
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

    val openBuffers = mutableMapOf<String, OpenBuffer>()
    var totalFetched = 0L

    flow.collect { requisition ->
      totalFetched += 1
      val reportId = extractReportId(requisition)
      if (reportId == null) {
        requisitionGrouper.refuseRequisitionToCmms(
          requisition,
          refusal {
            justification = Requisition.Refusal.Justification.SPEC_INVALID
            message = "Unable to parse MeasurementSpec"
          },
        )
        return@collect
      }

      val existing = openBuffers[reportId]
      if (existing == null) {
        openBuffers[reportId] =
          OpenBuffer(
            reportId = reportId,
            currentUpdateTime = requisition.updateTime,
            requisitions = mutableListOf(requisition),
          )
        return@collect
      }

      if (compareTimestamps(requisition.updateTime, existing.currentUpdateTime) > 0) {
        channel.send(ReportWorkUnit(existing.reportId, existing.requisitions.toList()))
        openBuffers[reportId] =
          OpenBuffer(
            reportId = reportId,
            currentUpdateTime = requisition.updateTime,
            requisitions = mutableListOf(requisition),
          )
        return@collect
      }

      existing.requisitions.add(requisition)
      if (existing.requisitions.size >= maxBufferedRequisitionsPerReport) {
        channel.send(ReportWorkUnit(existing.reportId, existing.requisitions.toList()))
        metrics.bufferSplits.add(1, dataProviderAttrs)
        openBuffers[reportId] =
          OpenBuffer(
            reportId = reportId,
            currentUpdateTime = existing.currentUpdateTime,
            requisitions = mutableListOf(),
          )
      }
    }

    for (buffer in openBuffers.values) {
      if (buffer.requisitions.isNotEmpty()) {
        channel.send(ReportWorkUnit(buffer.reportId, buffer.requisitions.toList()))
      }
    }
    return totalFetched
  }

  private suspend fun processReport(unit: ReportWorkUnit) {
    try {
      traceSuspending(
        spanName = SPAN_PROCESS_REPORT,
        attributes =
          Attributes.builder()
            .put(ATTR_DATA_PROVIDER_KEY, dataProviderName)
            .put(ATTR_REPORT_ID_KEY, unit.reportId)
            .build(),
      ) {
        processReportInner(unit)
      }
    } catch (e: CancellationException) {
      throw e
    } catch (e: Exception) {
      metrics.reportFailures.add(
        1,
        Attributes.builder()
          .put(ATTR_DATA_PROVIDER_KEY, dataProviderName)
          .put(ATTR_REPORT_ID_KEY, unit.reportId)
          .build(),
      )
      logger.log(Level.SEVERE, "Failed to process report ${unit.reportId} for $dataProviderName", e)
    }
  }

  private suspend fun processReportInner(unit: ReportWorkUnit) {
    val existingMetadata = listRequisitionMetadataByReportId(unit.reportId)
    val storedByGroupId =
      existingMetadata
        .filter { it.state == RequisitionMetadata.State.STORED }
        .groupBy { it.groupId }

    for ((existingGroupId, metadataList) in storedByGroupId) {
      val blobKey = blobKey(existingGroupId)
      if (storageClient.getBlob(blobKey) != null) continue
      val requisitionNames = metadataList.mapTo(mutableSetOf()) { it.cmmsRequisition }
      val matchingRequisitions = unit.requisitions.filter { it.name in requisitionNames }
      if (matchingRequisitions.isEmpty()) continue
      val rebuilt =
        try {
          requisitionGrouper.groupForReport(unit.reportId, matchingRequisitions, existingGroupId)
        } catch (e: InconsistentEventGroupSelectorsException) {
          logger.log(
            Level.WARNING,
            "Cannot rebuild missing blob for groupId=$existingGroupId report=${unit.reportId}",
            e,
          )
          null
        }
      if (rebuilt != null) {
        writeBlob(rebuilt)
        metrics.recoveryRebuilds.add(1, dataProviderAttrs)
      }
    }

    val existingNames = existingMetadata.mapTo(mutableSetOf()) { it.cmmsRequisition }
    val unregistered = unit.requisitions.filter { it.name !in existingNames }
    if (unregistered.isEmpty()) return

    val groupId = UUID.randomUUID().toString()
    val invalidRefusal = validateForReport(unit.reportId, unregistered)
    if (invalidRefusal != null) {
      refuseUnregisteredAndPersist(unregistered, groupId, invalidRefusal)
      return
    }

    val grouped =
      try {
        requisitionGrouper.groupForReport(unit.reportId, unregistered, groupId)
      } catch (e: InconsistentEventGroupSelectorsException) {
        val refusal = refusal {
          justification = Requisition.Refusal.Justification.UNFULFILLABLE
          message = e.message ?: "Invalid event group configuration"
        }
        refuseUnregisteredAndPersist(unregistered, groupId, refusal)
        return
      }
    if (grouped == null) return

    writeBlob(grouped)
    for (requisition in unregistered) {
      metadataThrottler.onReady { createRequisitionMetadata(requisition, groupId) }
    }
  }

  private fun validateForReport(
    reportId: String,
    requisitions: List<Requisition>,
  ): Requisition.Refusal? {
    for (requisition in requisitions) {
      try {
        requisitionValidator.validateRequisitionSpec(requisition)
      } catch (e: InvalidRequisitionException) {
        return e.refusal
      }
    }
    val modelLine = requisitions.first().measurementSpec.unpack<MeasurementSpec>().modelLine
    val mixedModelLines =
      requisitions.any { it.measurementSpec.unpack<MeasurementSpec>().modelLine != modelLine }
    if (mixedModelLines) {
      return refusal {
        justification = Requisition.Refusal.Justification.UNFULFILLABLE
        message = "Report $reportId cannot contain multiple model lines"
      }
    }
    return null
  }

  private suspend fun refuseUnregisteredAndPersist(
    requisitions: List<Requisition>,
    groupId: String,
    refusal: Requisition.Refusal,
  ) {
    for (requisition in requisitions) {
      requisitionGrouper.refuseRequisitionToCmms(requisition, refusal)
    }
    for (requisition in requisitions) {
      metadataThrottler.onReady {
        val metadata = createRequisitionMetadata(requisition, groupId)
        refuseRequisitionMetadata(metadata, refusal.message)
      }
    }
  }

  private suspend fun writeBlob(grouped: GroupedRequisitions) {
    val blobKey = blobKey(grouped.groupId)
    try {
      storageClient.writeBlob(blobKey, Any.pack(grouped).toByteString())
      metrics.storageWrites.add(1, dataProviderAttrs)
      logger.info(
        "Wrote grouped requisitions blob $blobKey for $dataProviderName " +
          "(groupId=${grouped.groupId}, requisitions=${grouped.requisitionsList.size})"
      )
    } catch (e: Exception) {
      metrics.storageFails.add(1, dataProviderAttrs)
      throw e
    }
  }

  private fun blobKey(groupId: String): String = "$storagePathPrefix/$groupId"

  private fun extractReportId(requisition: Requisition): String? {
    val measurementSpec: MeasurementSpec =
      try {
        requisition.measurementSpec.unpack()
      } catch (e: Exception) {
        logger.log(Level.WARNING, "Unable to parse MeasurementSpec for ${requisition.name}", e)
        return null
      }
    val report = measurementSpec.reportingMetadata.report
    return if (report.isBlank()) null else report
  }

  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private suspend fun listRequisitionMetadataByReportId(
    reportId: String
  ): List<RequisitionMetadata> {
    val results = mutableListOf<RequisitionMetadata>()
    val flow =
      requisitionMetadataStub
        .listResources { pageToken: String ->
          val request = listRequisitionMetadataRequest {
            parent = dataProviderName
            filter = ListRequisitionMetadataRequestKt.filter { report = reportId }
            pageSize = metadataPageSize
            this.pageToken = pageToken
          }
          val response: ListRequisitionMetadataResponse =
            metadataThrottler.onReady { requisitionMetadataStub.listRequisitionMetadata(request) }
          ResourceList(response.requisitionMetadataList, response.nextPageToken)
        }
        .flattenConcat()
    flow.collect { results.add(it) }
    return results
  }

  private suspend fun createRequisitionMetadata(
    requisition: Requisition,
    groupId: String,
  ): RequisitionMetadata {
    val metadata = requisitionMetadata {
      cmmsRequisition = requisition.name
      blobUri = "$blobUriPrefix/$storagePathPrefix/$groupId"
      blobTypeUrl = GROUPED_REQUISITION_BLOB_TYPE_URL
      this.groupId = groupId
      cmmsCreateTime = requisition.updateTime
      report = extractReportId(requisition).orEmpty()
    }
    val request = createRequisitionMetadataRequest {
      parent = dataProviderName
      requisitionMetadata = metadata
      requestId = UUID.randomUUID().toString()
    }
    return requisitionMetadataStub.createRequisitionMetadata(request)
  }

  private suspend fun refuseRequisitionMetadata(metadata: RequisitionMetadata, message: String) {
    val request = refuseRequisitionMetadataRequest {
      name = metadata.name
      etag = metadata.etag
      refusalMessage = message
    }
    requisitionMetadataStub.refuseRequisitionMetadata(request)
  }

  private val dataProviderAttrs: Attributes =
    Attributes.of(ATTR_DATA_PROVIDER_KEY, dataProviderName)

  companion object {
    private val logger: Logger = Logger.getLogger(RequisitionFetcher::class.java.name)
    private val ATTR_DATA_PROVIDER_KEY = AttributeKey.stringKey("edpa.data_provider_name")
    private val ATTR_REQUISITION_COUNT_KEY =
      AttributeKey.longKey("edpa.requisition_fetcher.requisition_count")
    private val ATTR_REPORT_ID_KEY = AttributeKey.stringKey("edpa.report_id")

    private const val SPAN_FETCH_REQUISITIONS = "edpa.requisition_fetcher.requisition_fetch"
    private const val SPAN_PROCESS_REPORT = "edpa.requisition_fetcher.process_report"
    private const val EVENT_FETCH_COMPLETED = "requisition_fetch_completed"

    private val GROUPED_REQUISITION_BLOB_TYPE_URL =
      ProtoReflection.getTypeUrl(GroupedRequisitions.getDescriptor())

    const val DEFAULT_METADATA_PAGE_SIZE: Int = 100
    const val DEFAULT_MAX_BUFFERED_REQUISITIONS_PER_REPORT: Int = 1000
    const val DEFAULT_WORKER_COUNT: Int = 16
    const val DEFAULT_CHANNEL_CAPACITY: Int = 64

    private fun compareTimestamps(a: Timestamp, b: Timestamp): Int {
      val cmpSeconds = a.seconds.compareTo(b.seconds)
      return if (cmpSeconds != 0) cmpSeconds else a.nanos.compareTo(b.nanos)
    }
  }
}
