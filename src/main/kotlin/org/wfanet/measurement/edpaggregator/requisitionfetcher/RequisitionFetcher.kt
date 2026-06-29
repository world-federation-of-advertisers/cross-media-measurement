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
import com.google.protobuf.util.Timestamps
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
import org.wfanet.measurement.common.api.grpc.listResourcesWithAdaptivePageSize
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
    var currentUpdateTime: com.google.protobuf.Timestamp,
    val requisitions: MutableList<Requisition> = mutableListOf(),
  )

  /**
   * Per-worker accumulator for STORED metadata rows whose blob is missing.
   *
   * A wedged group's requisitions may arrive across multiple [ReportWorkUnit]s for the same
   * reportId (different updateTimes, or buffer-cap splits). Per-report routing guarantees those
   * units land on the same worker; the worker collects matching requisitions across units and
   * rebuilds the blob only once the full expected set is present.
   *
   * Worker state, not shared.
   */
  private class PendingRecovery(
    val expected: Set<String>,
    val collected: MutableList<Requisition> = mutableListOf(),
  )

  /**
   * Fetches and stores unfulfilled requisitions for the configured data provider.
   *
   * Streams `UNFULFILLED` [Requisition]s from the Kingdom, buffers them per `(reportId,
   * updateTime)` tuple, dispatches each closed buffer onto a bounded channel, and processes each
   * report in one of a fixed-size pool of worker coroutines. Per-report failures are logged and
   * isolated so one bad report does not stop the run.
   *
   * @throws Exception if there is an error while listing requisitions from the Kingdom.
   */
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
        // Per-worker channels keyed by reportId.hashCode().mod(workerCount). All work units for
        // a given reportId land on the same worker, which lets the worker accumulate cross-unit
        // state (notably PendingRecovery) without races against peer workers.
        val perWorkerCapacity = (channelCapacity / workerCount).coerceAtLeast(1)
        val channels: List<Channel<ReportWorkUnit>> =
          List(workerCount) { Channel<ReportWorkUnit>(perWorkerCapacity) }
        val workers =
          channels.map { ch ->
            launch {
              val pendingRecovery = mutableMapOf<String, PendingRecovery>()
              val metadataCache = mutableMapOf<String, List<RequisitionMetadata>>()
              try {
                for (unit in ch) {
                  processReport(unit, pendingRecovery, metadataCache)
                }
              } finally {
                finalizePendingRecovery(pendingRecovery)
              }
            }
          }

        try {
          totalFetched = produceWorkUnits(channels)
        } finally {
          channels.forEach { it.close() }
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

  /**
   * Streams requisitions from the Kingdom and dispatches per-report work units onto [channels].
   *
   * Maintains an [OpenBuffer] per `reportId`. The Kingdom does not guarantee per-report monotonic
   * `updateTime` ordering in its `listRequisitions` response, so a buffer is closed and dispatched
   * on (a) *any* change to the report's `updateTime` (forward or backward), (b) the buffer hitting
   * [maxBufferedRequisitionsPerReport], or (c) the stream ending. Requisitions whose
   * [MeasurementSpec] cannot be parsed are refused to the Kingdom in line and not dispatched.
   *
   * Dispatch hashes `reportId` to a per-worker channel so all units for one report land on the same
   * worker. The producer can backpressure on a hot report's channel while other workers keep
   * draining.
   *
   * The `openBuffers` map is not pruned during streaming because Kingdom does not guarantee
   * per-report contiguity; its size therefore grows with the count of distinct reportIds seen in
   * the stream. Each entry is bounded by [maxBufferedRequisitionsPerReport] requisitions. Peak size
   * is reported via [RequisitionFetcherMetrics.openBufferHighWaterMark] for observability.
   *
   * @return the total number of requisitions consumed from the Kingdom stream.
   */
  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private suspend fun produceWorkUnits(channels: List<Channel<ReportWorkUnit>>): Long {
    val startingPageSize = responsePageSize ?: KINGDOM_LIST_REQUISITIONS_DEFAULT_PAGE_SIZE
    val flow: Flow<Requisition> =
      requisitionsStub
        .listResourcesWithAdaptivePageSize(
          startingPageSize = startingPageSize,
          minPageSize = MIN_LIST_REQUISITIONS_PAGE_SIZE,
          onPageSizeReduced = { oldSize, newSize ->
            logger.warning(
              "ListRequisitions returned RESOURCE_EXHAUSTED at page_size=$oldSize for " +
                "$dataProviderName; halving to $newSize and retrying"
            )
            metrics.pageSizeReductions.add(1, dataProviderAttrs)
          },
        ) { pageToken: String, pageSize: Int ->
          val request = listRequisitionsRequest {
            parent = dataProviderName
            filter = ListRequisitionsRequestKt.filter { states += Requisition.State.UNFULFILLED }
            this.pageSize = pageSize
            this.pageToken = pageToken
          }
          val response = requisitionsStub.listRequisitions(request)
          ResourceList(response.requisitionsList, response.nextPageToken)
        }
        .flattenConcat()

    val openBuffers = mutableMapOf<String, OpenBuffer>()
    var totalFetched = 0L
    var openBufferHighWater = 0

    suspend fun dispatch(buffer: OpenBuffer) {
      val unit = ReportWorkUnit(buffer.reportId, buffer.requisitions.toList())
      channels[channelIndexFor(buffer.reportId)].send(unit)
    }

    flow.collect { requisition ->
      totalFetched += 1
      val reportId = extractReportId(requisition)
      if (reportId == null) {
        requisitionGrouper.refuseRequisitionToCmms(
          requisition,
          refusal {
            justification = Requisition.Refusal.Justification.SPEC_INVALID
            message =
              "MeasurementSpec missing or has no reportingMetadata.report; unable to extract " +
                "report id"
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
        if (openBuffers.size > openBufferHighWater) openBufferHighWater = openBuffers.size
        return@collect
      }

      if (Timestamps.compare(requisition.updateTime, existing.currentUpdateTime) != 0) {
        dispatch(existing)
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
        dispatch(existing)
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
        dispatch(buffer)
      }
    }
    metrics.openBufferHighWaterMark.record(openBufferHighWater.toLong(), dataProviderAttrs)
    return totalFetched
  }

  private fun channelIndexFor(reportId: String): Int = reportId.hashCode().mod(workerCount)

  /**
   * Runs [processReportInner] for one [ReportWorkUnit] inside a trace span, isolating any failure
   * to this report. [CancellationException] is rethrown; any other exception is logged and counted
   * in [RequisitionFetcherMetrics.reportFailures]. [pendingRecovery] is the per-worker accumulator
   * shared across all units processed by this worker.
   */
  private suspend fun processReport(
    unit: ReportWorkUnit,
    pendingRecovery: MutableMap<String, PendingRecovery>,
    metadataCache: MutableMap<String, List<RequisitionMetadata>>,
  ) {
    try {
      traceSuspending(
        spanName = SPAN_PROCESS_REPORT,
        attributes =
          Attributes.builder()
            .put(ATTR_DATA_PROVIDER_KEY, dataProviderName)
            .put(ATTR_REPORT_ID_KEY, unit.reportId)
            .build(),
      ) {
        processReportInner(unit, pendingRecovery, metadataCache)
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

  /**
   * Persists [unit]'s requisitions for a single report.
   *
   * ### High-Level Flow
   * 1. List existing [RequisitionMetadata] for the report.
   * 2. For any STORED metadata whose blob is missing in [storageClient], accumulate the matching
   *    requisitions from this unit into [pendingRecovery] keyed by the existing `groupId`. When the
   *    accumulator has collected every requisition the STORED group expects, rebuild the blob and
   *    remove the entry. Partial accumulators stay until a later unit completes them, or are
   *    surfaced as incomplete in [finalizePendingRecovery] when the channel closes.
   * 3. For requisitions that are not yet recorded in metadata, validate them as a group (model-line
   *    consistency, requisition-spec decryption). On invalid input, refuse each requisition to the
   *    Kingdom and persist `REFUSED` metadata.
   * 4. On valid input, group the requisitions in memory and **write the blob first**, then create
   *    `STORED` metadata for each requisition. The blob-first ordering ensures a mid-run failure
   *    can only leave a recoverable state (blob without metadata), never the wedge state (metadata
   *    without blob).
   */
  private suspend fun processReportInner(
    unit: ReportWorkUnit,
    pendingRecovery: MutableMap<String, PendingRecovery>,
    metadataCache: MutableMap<String, List<RequisitionMetadata>>,
  ) {
    val existingMetadata =
      metadataCache.getOrPut(unit.reportId) { listRequisitionMetadataByReportId(unit.reportId) }
    val storedByGroupId =
      existingMetadata
        .filter { it.state == RequisitionMetadata.State.STORED }
        .groupBy { it.groupId }

    for ((existingGroupId, metadataList) in storedByGroupId) {
      val blobKey = blobKey(existingGroupId)
      if (storageClient.getBlob(blobKey) != null) continue
      val expectedNames = metadataList.mapTo(mutableSetOf()) { it.cmmsRequisition }
      val matchingHere = unit.requisitions.filter { it.name in expectedNames }
      // Always enter the wedged group into pendingRecovery so finalizePendingRecovery can surface
      // it via the recovery_skipped_incomplete counter — including the case where this run sees
      // zero matching requisitions for the group.
      val pending =
        pendingRecovery.getOrPut(existingGroupId) { PendingRecovery(expected = expectedNames) }
      pending.collected += matchingHere
      if (pending.collected.isEmpty()) continue

      if (pending.collected.size >= pending.expected.size) {
        val rebuilt =
          try {
            requisitionGrouper.groupForReport(
              unit.reportId,
              pending.collected.toList(),
              existingGroupId,
            )
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
        pendingRecovery.remove(existingGroupId)
      }
    }

    val existingNames = existingMetadata.mapTo(mutableSetOf()) { it.cmmsRequisition }
    val unregistered = unit.requisitions.filter { it.name !in existingNames }
    if (unregistered.isEmpty()) return

    val groupId = UUID.randomUUID().toString()
    val invalidRefusal = validateForReport(unit.reportId, unregistered)
    if (invalidRefusal != null) {
      refuseUnregisteredAndPersist(unregistered, groupId, invalidRefusal)
      metadataCache.remove(unit.reportId)
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
        metadataCache.remove(unit.reportId)
        return
      } ?: return

    writeBlob(grouped)
    for (requisition in unregistered) {
      metadataThrottler.onReady { createRequisitionMetadata(requisition, groupId) }
    }
    metadataCache.remove(unit.reportId)
  }

  /**
   * Logs and counts every still-incomplete [PendingRecovery] at channel close. An incomplete entry
   * means the stream did not contain every requisition advertised by the STORED metadata for the
   * group — typically because some are no longer `UNFULFILLED` (e.g. FULFILLED elsewhere) — and
   * therefore cannot be rebuilt from this run's data alone. Operator visibility, no automatic fix.
   */
  private fun finalizePendingRecovery(pendingRecovery: Map<String, PendingRecovery>) {
    for ((groupId, pending) in pendingRecovery) {
      val level = if (pending.collected.isEmpty()) Level.SEVERE else Level.WARNING
      logger.log(
        level,
        "Recovery incomplete for groupId=$groupId: ${pending.collected.size}/" +
          "${pending.expected.size} requisitions available; blob will remain missing",
      )
      metrics.recoverySkippedIncomplete.add(1, dataProviderAttrs)
    }
  }

  /**
   * Validates a report's unregistered [requisitions] as a group.
   *
   * Returns the first [Requisition.Refusal] that should be applied to *every* requisition in the
   * report, or `null` if all are valid. Surfaces individual requisition-spec parse failures as well
   * as cross-requisition consistency failures (currently: mixed model lines).
   */
  private fun validateForReport(
    reportId: String,
    requisitions: List<Requisition>,
  ): Requisition.Refusal? {
    // MeasurementSpec is guaranteed parseable here: the stream producer's extractReportId
    // already unpacked and discarded any requisition with an unparseable spec.
    for (requisition in requisitions) {
      try {
        requisitionValidator.validateRequisitionSpec(requisition)
      } catch (e: InvalidRequisitionException) {
        return e.refusal
      }
    }
    val measurementSpecs = requisitions.map { it.measurementSpec.unpack<MeasurementSpec>() }
    val modelLine = measurementSpecs.first().modelLine
    if (measurementSpecs.any { it.modelLine != modelLine }) {
      return refusal {
        justification = Requisition.Refusal.Justification.UNFULFILLABLE
        message = "Report $reportId cannot contain multiple model lines"
      }
    }
    return null
  }

  /**
   * Refuses every requisition in [requisitions] to the Kingdom and persists corresponding `REFUSED`
   * [RequisitionMetadata] entries under the shared [groupId].
   */
  private suspend fun refuseUnregisteredAndPersist(
    requisitions: List<Requisition>,
    groupId: String,
    refusal: Requisition.Refusal,
  ) {
    for (requisition in requisitions) {
      requisitionGrouper.refuseRequisitionToCmms(requisition, refusal)
    }
    for (requisition in requisitions) {
      val metadata = metadataThrottler.onReady { createRequisitionMetadata(requisition, groupId) }
      metadataThrottler.onReady { refuseRequisitionMetadata(metadata, refusal.message) }
    }
  }

  /**
   * Writes [grouped] to [storageClient]. Increments storage-write or storage-fail counters and
   * rethrows on failure so the per-report worker can surface the error.
   */
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

  /**
   * Returns the report ID embedded in [requisition]'s [MeasurementSpec], or `null` if the spec
   * cannot be parsed or has no report set.
   */
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

  /**
   * Lists all [RequisitionMetadata] for a report by paging through the Requisition Metadata
   * Service. Each page is gated by [metadataThrottler].
   */
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

  /**
   * Creates a `STORED` [RequisitionMetadata] entry for [requisition] under [groupId], with a blob
   * URI computed from [blobUriPrefix] and [storagePathPrefix].
   */
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

  /**
   * Marks [metadata] as `REFUSED` with the given [message] via the Requisition Metadata Service.
   */
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
    const val MIN_LIST_REQUISITIONS_PAGE_SIZE: Int = 1
    private const val KINGDOM_LIST_REQUISITIONS_DEFAULT_PAGE_SIZE: Int = 10
  }
}
