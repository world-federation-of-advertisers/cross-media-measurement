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
import io.grpc.Status
import io.grpc.StatusException
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.Span
import java.time.Duration
import java.util.UUID
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.time.TimeSource
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
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
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.createRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.refuseRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.requisitionMetadata
import org.wfanet.measurement.storage.StorageClient

/**
 * Fetches requisitions from the Kingdom and persists them, grouped by report, to a [StorageClient].
 *
 * The fetcher streams requisitions through a bounded channel into a single consumer coroutine. The
 * consumer:
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
 * @property flushInterval wall-clock period between forced drains of every open report buffer. This
 *   is the primary dispatch trigger: because the stream carries no per-report completeness signal
 *   and may outlive the Cloud Run instance (or never fully drain), the fetcher cannot wait for
 *   end-of-stream to emit blobs. Draining periodically bounds data-at-risk and feeds downstream
 *   continuously, at the cost of splitting any report whose requisitions span more than one window.
 * @property maxTotalBufferedBytes global upper bound on the serialized bytes across all open report
 *   buffers. When reached, every open buffer is flushed immediately (a memory backstop for a fast
 *   window between periodic drains). Bytes-based because per-requisition size varies widely (the
 *   encrypted spec dominates) and the budget that matters is bytes, not count.
 * @property channelCapacity capacity of the channel between the stream producer and the consumer.
 * @property maxRequisitionsPerGroup maximum requisitions per grouped-requisitions blob and its
 *   metadata batch. A report with more than this is written across multiple groups (blobs),
 *   bounding each metadata `BatchCreate` well under Spanner's per-transaction mutation limit. There
 *   is no upstream cap on requisitions per report; in practice a report has far fewer than this, so
 *   it is a safety bound rather than a routine split.
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
  private val flushInterval: Duration = DEFAULT_FLUSH_INTERVAL,
  private val maxTotalBufferedBytes: Long = DEFAULT_MAX_TOTAL_BUFFERED_BYTES,
  private val maxRequisitionsPerGroup: Int = DEFAULT_MAX_REQUISITIONS_PER_GROUP,
  private val channelCapacity: Int = DEFAULT_CHANNEL_CAPACITY,
  private val metrics: RequisitionFetcherMetrics = RequisitionFetcherMetrics.Default,
) {

  private data class ReportWorkUnit(val reportId: String, val requisitions: List<Requisition>)

  private class OpenBuffer(
    val reportId: String,
    val requisitions: MutableList<Requisition> = mutableListOf(),
    var bytes: Long = 0L,
  )

  /**
   * Accumulator for STORED metadata rows whose blob is missing.
   *
   * A wedged group's requisitions may arrive across multiple [ReportWorkUnit]s for the same
   * reportId (a report split across periodic drains or a byte-cap flush). The single consumer
   * collects matching requisitions across units and rebuilds the blob only once the full expected
   * set is present.
   *
   * [collected] is keyed by [Requisition.getName] so that re-emissions of the same requisition
   * across units (e.g. Kingdom-side `updateTime` drift between pages causing the same requisition
   * to appear twice in the stream) are deduplicated. Without this, [collected]'s `size` could reach
   * [expected]`.size` while still missing distinct requisitions, triggering a premature rebuild
   * that writes a blob inconsistent with the metadata it shadows.
   *
   * Consumer state, not shared.
   */
  private class PendingRecovery(
    val expected: Set<String>,
    val collected: MutableMap<String, Requisition> = mutableMapOf(),
  )

  /**
   * Fetches and stores unfulfilled requisitions for the configured data provider.
   *
   * Streams `UNFULFILLED` [Requisition]s from the Kingdom, buffers them by `reportId`, drains each
   * report's buffer onto a bounded channel (periodically and at a total-bytes cap), and processes
   * the drained work units in a single consumer coroutine. Per-report failures are logged and
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
        // Single queue, single consumer. All report buffers accumulate in the producer and drain to
        // one consumer, so `pendingRecovery` and `metadataCache` are one map each (no per-consumer
        // replication) and there is no cross-consumer routing to reason about.
        val channel = Channel<ReportWorkUnit>(channelCapacity)
        val consumer = launch {
          val pendingRecovery = mutableMapOf<String, PendingRecovery>()
          val metadataCache = mutableMapOf<String, List<RequisitionMetadata>>()
          try {
            for (unit in channel) {
              processReport(unit, pendingRecovery, metadataCache)
            }
          } finally {
            finalizePendingRecovery(pendingRecovery)
          }
        }

        try {
          totalFetched = produceWorkUnits(channel)
        } finally {
          channel.close()
        }
        consumer.join()
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
   * Streams requisitions from the Kingdom and dispatches per-report work units onto [channel].
   *
   * Maintains one [OpenBuffer] per `reportId` and groups strictly by `reportId` — `updateTime` does
   * not gate grouping. A report's requisitions are transitioned to `UNFULFILLED` per Measurement
   * (each Kingdom `SetParticipantRequisitionParams` stamps a fresh commit timestamp), so a
   * multi-metric report legitimately spans many distinct `updateTime`s; keying the buffer on
   * `updateTime` would fragment every such report into one blob per requisition. Requisitions whose
   * [MeasurementSpec] cannot be parsed are refused to the Kingdom in line and not buffered.
   *
   * Buffers are dispatched by two triggers, neither of which is end-of-stream (the stream carries
   * no per-report completeness signal and can outlive the Cloud Run instance or never fully drain,
   * so waiting for it to end would starve downstream and risk emitting nothing):
   * 1. **Periodic drain** — every [flushInterval], a background ticker flushes *all* open buffers.
   *    This is the primary trigger; it bounds data-at-risk and feeds downstream continuously.
   * 2. **Total-bytes backstop** — when the serialized bytes across all open buffers reach
   *    [maxTotalBufferedBytes], all buffers are flushed immediately to bound memory between ticks.
   *
   * A report whose requisitions all arrive within one window becomes a single blob; a report
   * straddling K windows (or a byte-cap flush) is split across ~K blobs. Splitting is the accepted
   * cost of incremental progress and is counted in [RequisitionFetcherMetrics.bufferSplits].
   *
   * `openBuffers` is guarded by a [Mutex] because the periodic-drain ticker and the stream
   * collector run concurrently and both mutate it. Peak map cardinality and total buffered bytes
   * are reported via [RequisitionFetcherMetrics.openBufferHighWaterMark] and
   * [RequisitionFetcherMetrics.bufferedBytesHighWaterMark].
   *
   * @return the total number of requisitions consumed from the Kingdom stream.
   */
  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private suspend fun produceWorkUnits(channel: Channel<ReportWorkUnit>): Long = coroutineScope {
    // Kingdom's internal streamRequisitions (which ListRequisitions proxies to) orders pages
    // globally by `UpdateTime ASC, ExternalDataProviderId ASC, ExternalRequisitionId ASC` (see
    // Kingdom's StreamRequisitions query). Grouping does not depend on that ordering — buffers are
    // keyed by reportId and flushed by the periodic ticker and the total-bytes backstop — but the
    // ordering does mean a report's requisitions tend to arrive contiguously, so most reports are
    // collected and emitted as a single blob between drains.
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
          val response =
            try {
              requisitionsStub.listRequisitions(request)
            } catch (e: StatusException) {
              // RESOURCE_EXHAUSTED is the adaptive page-size signal — propagate raw so the
              // listResourcesWithAdaptivePageSize wrapper can detect it and halve. Wrap
              // everything else with context to disambiguate failures from getEventGroup /
              // metadata RPCs that share the StatusException type.
              if (e.status.code == Status.Code.RESOURCE_EXHAUSTED) throw e
              throw Exception("Error listing requisitions for $dataProviderName", e)
            }
          ResourceList(response.requisitionsList, response.nextPageToken)
        }
        .flattenConcat()

    val openBuffers = mutableMapOf<String, OpenBuffer>()
    val buffersMutex = Mutex()
    var totalFetched = 0L
    var openBufferHighWater = 0
    var totalBufferedBytes = 0L
    var bufferedBytesHighWater = 0L

    // Must hold buffersMutex. Atomically snapshots every open buffer into work units, clears the
    // map, and resets the running byte total. Does NOT send: this function is non-suspending so the
    // lock is never held across the suspending `channel.send`. Callers send the returned units
    // outside the lock, so a slow consumer backing up the channel cannot stall the stream collector
    // (which needs the same lock to keep buffering). Backpressure is provided solely by the
    // channel.
    fun drainAll(): List<ReportWorkUnit> {
      if (openBuffers.isEmpty()) return emptyList()
      val units = openBuffers.values.map { ReportWorkUnit(it.reportId, it.requisitions.toList()) }
      openBuffers.clear()
      totalBufferedBytes = 0L
      return units
    }

    // Snapshots the open buffers under the lock, then sends the work units outside it (see
    // [drainAll]). Used by the periodic ticker, the total-bytes backstop, and the final drain.
    suspend fun drainAndSend() {
      for (unit in buffersMutex.withLock { drainAll() }) channel.send(unit)
    }

    // Periodic drain: the primary dispatch trigger. Runs until the collector completes and cancels
    // it. Each tick flushes every open buffer so downstream sees blobs continuously, and no report
    // is held longer than roughly one interval regardless of how long the stream takes to read.
    val drainTicker = launch {
      while (isActive) {
        delay(flushInterval.toMillis())
        // The delay above is the cancellation point that stops the ticker. Once a drain has removed
        // buffers from the map, its sends must complete (NonCancellable) so cancellation between
        // drainAll and send cannot drop already-drained units — they would otherwise be lost, since
        // the final drain no longer sees them.
        withContext(NonCancellable) { drainAndSend() }
      }
    }

    try {
      flow.collect { requisition ->
        // Counts every requisition streamed from the Kingdom, including those refused below for an
        // unparseable spec. Only this (single) collector mutates totalFetched, so no lock is
        // needed.
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

        val requisitionBytes = requisition.serializedSize.toLong()
        val overCap =
          buffersMutex.withLock {
            val existing = openBuffers[reportId]
            if (existing == null) {
              openBuffers[reportId] =
                OpenBuffer(
                  reportId = reportId,
                  requisitions = mutableListOf(requisition),
                  bytes = requisitionBytes,
                )
            } else {
              existing.requisitions.add(requisition)
              existing.bytes += requisitionBytes
            }
            totalBufferedBytes += requisitionBytes
            if (openBuffers.size > openBufferHighWater) openBufferHighWater = openBuffers.size
            if (totalBufferedBytes > bufferedBytesHighWater) {
              bufferedBytesHighWater = totalBufferedBytes
            }
            totalBufferedBytes >= maxTotalBufferedBytes
          }
        // Memory backstop between periodic drains.
        if (overCap) drainAndSend()
      }
    } finally {
      // cancelAndJoin (not cancel) so the ticker is fully stopped before the final drain — no drain
      // runs concurrently with it, and any in-flight NonCancellable send completes first.
      drainTicker.cancelAndJoin()
      // Record the memory high-water gauges here (in finally, before the final drain) so they are
      // emitted even when flow.collect throws mid-run — a failed run is exactly when the
      // buffer-cardinality and bytes-at-risk telemetry is most useful. The final drain does not
      // change these peaks, so recording before it yields the same values.
      metrics.openBufferHighWaterMark.record(openBufferHighWater.toLong(), dataProviderAttrs)
      metrics.bufferedBytesHighWaterMark.record(bufferedBytesHighWater, dataProviderAttrs)
    }

    // Final drain of whatever remains when the stream ends within the instance lifetime.
    drainAndSend()

    totalFetched
  }

  /**
   * Runs [processReportInner] for one [ReportWorkUnit] inside a trace span, isolating any failure
   * to this report. [CancellationException] is rethrown; any other exception is logged and counted
   * in [RequisitionFetcherMetrics.reportFailures] with an `error_type` label, and the run continues
   * with the next unit.
   *
   * The intentional swallow keeps one bad report from poisoning the whole run. Per-report failures
   * are not fatal because the requisitions remain `UNFULFILLED` in the Kingdom: the next scheduled
   * invocation re-streams them and re-runs the report from scratch, self-healing transient failures
   * (metadata-service `UNAVAILABLE`, Spanner `ABORTED`, GCS hiccup) on the next run.
   *
   * Operationally: the `report_failures` counter (with `error_type` label) is the alerting signal.
   * A non-transient exception type (e.g. `IllegalStateException`, `NullPointerException`) recurring
   * on the same `(data_provider, report_id)` across multiple runs is the smoking gun for a code bug
   * that won't self-heal. Transient exception types (`StatusException` with `UNAVAILABLE` /
   * `ABORTED` / `DEADLINE_EXCEEDED`) recovering on the next run are normal and should not page.
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
          .put(ATTR_ERROR_TYPE_KEY, errorTypeName(e))
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
      matchingHere.forEach { pending.collected.putIfAbsent(it.name, it) }
      if (pending.collected.isEmpty()) continue

      if (pending.collected.size >= pending.expected.size) {
        val rebuilt =
          try {
            requisitionGrouper.groupForReport(
              unit.reportId,
              pending.collected.values.toList(),
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
          // No metadata mutated on the recovery path (the blob is rebuilt for already-existing
          // STORED rows), so the consumer-local metadataCache stays accurate and is intentionally
          // not invalidated here.
        }
        pendingRecovery.remove(existingGroupId)
      }
    }

    val existingNames = existingMetadata.mapTo(mutableSetOf()) { it.cmmsRequisition }
    val unregistered = unit.requisitions.filter { it.name !in existingNames }
    if (unregistered.isEmpty()) return

    // Any persist path below mutates RequisitionMetadata for this report, so invalidate the
    // consumer-local cache unconditionally — including on partial-progress exceptions, where the
    // cache otherwise retains a pre-failure snapshot that hides rows just persisted.
    try {
      val invalidRefusal = validateForReport(unit.reportId, unregistered)
      if (invalidRefusal != null) {
        refuseUnregisteredAndPersist(unregistered, unit.reportId, invalidRefusal)
        return
      }

      // Split the report's requisitions into groups of at most [maxRequisitionsPerGroup],
      // each with its own groupId, blob, and atomic metadata batch. There is no upstream
      // limit on requisitions per report; in practice a report has far fewer than one
      // chunk, so the common case is a single group (identical to no chunking). The cap
      // bounds the metadata batch's Spanner mutation count (rows x columns x secondary
      // indexes) well under the ~80k per-transaction limit, so an unusually large report
      // cannot produce a batch that fails every run and wedges the report. Each chunk's
      // (blob, metadata) pair is internally consistent, so a mid-report failure only leaves
      // already-persisted chunks (fully consistent) plus at most one benign orphan blob
      // (blob without metadata), recovered on a later run — the same recoverable state the
      // single-group path already relies on.
      var priorBlobForReport = storedByGroupId.isNotEmpty()
      for (chunk in unregistered.chunked(maxRequisitionsPerGroup)) {
        val groupId = UUID.randomUUID().toString()
        val grouped =
          try {
            requisitionGrouper.groupForReport(unit.reportId, chunk, groupId)
          } catch (e: InconsistentEventGroupSelectorsException) {
            val refusal = refusal {
              justification = Requisition.Refusal.Justification.UNFULFILLABLE
              message = e.message ?: "Invalid event group configuration"
            }
            refuseUnregisteredAndPersist(chunk, unit.reportId, refusal)
            continue
          } ?: continue

        // Every blob beyond the report's first (a prior STORED group from any run, or an earlier
        // chunk this run) means the report is written across more than one blob: a split.
        if (priorBlobForReport) {
          metrics.bufferSplits.add(1, dataProviderAttrs)
        }
        priorBlobForReport = true
        writeBlob(grouped)
        // TODO(world-federation-of-advertisers/cross-media-measurement#4119): A crash between
        //  writeBlob and this batch call leaves an orphan blob (blob present, zero metadata).
        //  The orphan is benign for fetcher correctness but currently causes ResultsFulfiller to
        //  fail loudly at ResultsFulfiller.kt:160 and dead-letter the work item. Downstream fix:
        //  change ResultsFulfiller to log + skip rather than throw on the empty-metadata case.
        batchCreateRequisitionMetadataForGroup(chunk, groupId, unit.reportId)
      }
    } finally {
      metadataCache.remove(unit.reportId)
    }
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
   * [RequisitionMetadata] entries.
   *
   * Requisitions are chunked into groups of at most [maxRequisitionsPerGroup], each under its own
   * generated groupId, so each `BatchCreateRequisitionMetadata` stays within Spanner's
   * per-transaction mutation limit (see [batchCreateRequisitionMetadataForGroup]). Each batch is
   * atomic, so a crash cannot leave a partial set of metadata rows within a group. This path writes
   * no blob — it only persists REFUSED metadata and refuses each requisition to the Kingdom.
   */
  private suspend fun refuseUnregisteredAndPersist(
    requisitions: List<Requisition>,
    reportId: String,
    refusal: Requisition.Refusal,
  ) {
    metrics.reportRefusals.add(
      1,
      Attributes.builder()
        .put(ATTR_DATA_PROVIDER_KEY, dataProviderName)
        .put(ATTR_REPORT_ID_KEY, reportId)
        .put(ATTR_JUSTIFICATION_KEY, refusal.justification.name)
        .build(),
    )
    for (requisition in requisitions) {
      requisitionGrouper.refuseRequisitionToCmms(requisition, refusal)
    }
    // Chunked for the same Spanner-mutation-limit reason as the store path: a refused report can be
    // arbitrarily large, and each metadata batch must stay well under the per-transaction limit. No
    // bufferSplits here: the refuse path writes no blobs, so there is no fulfillment data to
    // fragment.
    for (chunk in requisitions.chunked(maxRequisitionsPerGroup)) {
      val groupId = UUID.randomUUID().toString()
      val createdMetadata = batchCreateRequisitionMetadataForGroup(chunk, groupId, reportId)
      for (metadata in createdMetadata) {
        metadataThrottler.onReady { refuseRequisitionMetadata(metadata, refusal.message) }
      }
    }
  }

  /**
   * Writes [grouped] to [storageClient]. Increments storage-write or storage-fail counters and
   * rethrows on failure so the consumer can surface the error.
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
   * Builds the [RequisitionMetadata] for [requisition] under [groupId] for [reportId], with a blob
   * URI computed from [blobUriPrefix] and [storagePathPrefix]. [reportId] is passed in rather than
   * re-derived from the requisition's [MeasurementSpec]: the producer already extracted and
   * validated it once per requisition (see [extractReportId] in `produceWorkUnits`).
   */
  private fun buildRequisitionMetadata(
    requisition: Requisition,
    groupId: String,
    reportId: String,
  ): RequisitionMetadata {
    return requisitionMetadata {
      cmmsRequisition = requisition.name
      blobUri = "$blobUriPrefix/$storagePathPrefix/$groupId"
      blobTypeUrl = GROUPED_REQUISITION_BLOB_TYPE_URL
      this.groupId = groupId
      cmmsCreateTime = requisition.updateTime
      report = reportId
    }
  }

  /**
   * Atomically creates `STORED` [RequisitionMetadata] entries for every [requisition] in
   * [requisitions] under the shared [groupId] for [reportId], in a single [Throttler]-gated
   * `BatchCreateRequisitionMetadata` call. Returns the list of created entries in the same order as
   * [requisitions].
   *
   * Wedge fix: the server backs this RPC with a single Spanner read-write transaction, so all
   * metadata rows commit together or none do. A crash between [writeBlob] and this call leaves the
   * benign zero-metadata orphan (recoverable by the next run); a crash mid-call aborts the
   * transaction, leaving zero metadata rows. Either way ResultsFulfiller never sees a blob whose
   * `requisitionsList` references a name with no corresponding metadata row.
   *
   * This issues one Spanner read-write transaction per call. Each requisition writes a
   * RequisitionMetadata row (~14 columns, 8 secondary indexes) plus a RequisitionMetadataActions
   * row recording the UNSPECIFIED -> STORED transition (~6 columns, 3 secondary indexes) in the
   * same transaction. Spanner counts a mutation per written cell and per index-entry cell, so the
   * cost is ~30 mutations/requisition (one entry per index) up to ~64 (every index cell), against
   * the ~80k per-transaction limit. Callers bound the batch to [DEFAULT_MAX_REQUISITIONS_PER_GROUP]
   * requisitions per group by splitting oversized reports across groups, so even under the
   * worst-case per-cell accounting a single batch stays under the limit and this call never has to
   * chunk internally.
   *
   * No-op when [requisitions] is empty.
   */
  private suspend fun batchCreateRequisitionMetadataForGroup(
    requisitions: List<Requisition>,
    groupId: String,
    reportId: String,
  ): List<RequisitionMetadata> {
    if (requisitions.isEmpty()) return emptyList()
    val response =
      metadataThrottler.onReady {
        requisitionMetadataStub.batchCreateRequisitionMetadata(
          batchCreateRequisitionMetadataRequest {
            parent = dataProviderName
            requests +=
              requisitions.map { requisition ->
                createRequisitionMetadataRequest {
                  parent = dataProviderName
                  requisitionMetadata = buildRequisitionMetadata(requisition, groupId, reportId)
                  // Deterministic so a same-run retry of the same batch is server-side
                  // idempotent: matching requestId returns the existing row instead of
                  // ALREADY_EXISTS on (cmmsRequisition, groupId). The server requires
                  // request_id to parse as a UUID (RequisitionMetadataService.kt
                  // validateRequisitionMetadataRequest), so derive a UUID v3 from the
                  // (cmmsRequisition, groupId) pair via name-based hashing — stable per pair.
                  requestId =
                    UUID.nameUUIDFromBytes("${requisition.name}/$groupId".toByteArray()).toString()
                }
              }
          }
        )
      }
    return response.requisitionMetadataList
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
    private val ATTR_ERROR_TYPE_KEY = AttributeKey.stringKey("edpa.requisition_fetcher.error_type")
    private val ATTR_JUSTIFICATION_KEY =
      AttributeKey.stringKey("edpa.requisition_fetcher.justification")

    private fun errorTypeName(e: Throwable): String =
      e::class.simpleName ?: e::class.java.simpleName ?: e::class.java.name

    private const val SPAN_FETCH_REQUISITIONS = "edpa.requisition_fetcher.requisition_fetch"
    private const val SPAN_PROCESS_REPORT = "edpa.requisition_fetcher.process_report"
    private const val EVENT_FETCH_COMPLETED = "requisition_fetch_completed"

    private val GROUPED_REQUISITION_BLOB_TYPE_URL =
      ProtoReflection.getTypeUrl(GroupedRequisitions.getDescriptor())

    const val DEFAULT_METADATA_PAGE_SIZE: Int = 100
    const val DEFAULT_MAX_TOTAL_BUFFERED_BYTES: Long = 256L * 1024L * 1024L
    // Caps requisitions per metadata batch to bound the Spanner mutation count. Each
    // requisition writes a RequisitionMetadata row (~14 columns, 8 indexes) and a
    // RequisitionMetadataActions row (~6 columns, 3 indexes) in one transaction — ~30
    // mutations/requisition counting one entry per index, up to ~64 counting every index
    // cell. At 1000 that is ~30k-64k mutations, under the ~80k per-transaction limit even in
    // the worst case. In practice a report has fewer requisitions than this, so it is a
    // safety cap rather than a routine split.
    const val DEFAULT_MAX_REQUISITIONS_PER_GROUP: Int = 1000
    val DEFAULT_FLUSH_INTERVAL: Duration = Duration.ofMinutes(5)
    const val DEFAULT_CHANNEL_CAPACITY: Int = 4
    const val MIN_LIST_REQUISITIONS_PAGE_SIZE: Int = 1
    private const val KINGDOM_LIST_REQUISITIONS_DEFAULT_PAGE_SIZE: Int = 10
  }
}
