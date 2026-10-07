/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.vidlabeling

import com.google.protobuf.util.Timestamps
import io.grpc.Status
import io.grpc.StatusException
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.Span
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.flattenConcat
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.common.telemetry.XmmTraceAttributes
import org.wfanet.measurement.common.telemetry.XmmTracing
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.VidLabelingRpcThrottlers
import org.wfanet.measurement.edpaggregator.telemetry.Tracing
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceAttributes
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceLogging
import org.wfanet.measurement.edpaggregator.v1alpha.ListPoolAssignmentJobsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankerJobsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadCorrectionCandidatesRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListVidLabelingJobsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJob
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJob
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadCorrectionCandidateServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJob
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.listPoolAssignmentJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRankerJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadCorrectionCandidatesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listVidLabelingJobsRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.createWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.getWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.StorageClient

/**
 * Monitors the VID labeling pipeline for one `DataProvider` and drives dispatch sequencing.
 *
 * Cloud Scheduler invokes [VidLabelingMonitorFunction] periodically; per DataProvider it builds one
 * [VidLabelingMonitor] and calls [runDispatch] (fast dispatch cadence) or [runHealth] (slow health
 * cadence). This first iteration (#3958) implements:
 * - **Dispatch sequencing:** delegated to the shared [VidLabelingDispatchSequencer], which both
 *   this monitor (the periodic backstop) and [VidLabelingDispatcher] (the upload-triggered fast
 *   path) call. The sequencer allows different model lines for the same DataProvider to run
 *   concurrently, but enforces at most one active upload per `(DataProvider, ModelLine)`. It starts
 *   Phase-0/Phase-2 work for the oldest eligible `CREATED` model-line states. Keeping that logic in
 *   one place means the sequencing rule lives in exactly one component with one set of tests.
 * - **Failure + staleness monitoring:** uploads stuck in a non-terminal state past
 *   [stalenessThreshold] are surfaced via [VidLabelingMonitorMetrics.uploadsStuckGauge], and
 *   uploads with a `FAILED` model line via [VidLabelingMonitorMetrics.failedUploadsGauge], for
 *   duration-window alerting (no per-tick `SEVERE`, to avoid re-paging until manual recovery).
 *
 * The health pass recovers stalled phase transitions (`POOL_ASSIGNING → RANKING → LABELING →
 * AVAILABILITY_SYNCING → COMPLETED`) by replaying one completed child WorkItem.
 *
 * @param rawImpressionUploadStub stub for `RawImpressionUploadService`.
 * @param rawImpressionUploadModelLineStub stub for `RawImpressionUploadModelLineService`.
 * @param correctionCandidateStub stub for `RawImpressionUploadCorrectionCandidateService`.
 * @param poolAssignmentJobStub stub for `PoolAssignmentJobService`.
 * @param dispatchSequencer shared sequencer that performs dispatch for this DataProvider.
 * @param dataProviderName resource name of the `DataProvider` this monitor scans.
 * @param stalenessThreshold non-terminal uploads older than this are flagged as stuck.
 * @param rawImpressionsStorageRootUri absolute URI of this EDP's raw-impression storage root.
 * @param rawImpressionsBlobPrefix bucket-relative prefix containing this EDP's raw uploads.
 * @param rawInputQuietPeriod age after which an unfinalized raw-input condition is alertable.
 * @param rawImpressionsExcludedBlobPrefixes known non-raw descendants omitted from classification.
 * @param vidLabeledImpressionsBlobPrefix URI prefix for labeled output.
 * @param rpcThrottlers process-scoped rate limiters shared with the dispatch sequencer.
 * @param clock clock used for staleness evaluation.
 * @param metrics OpenTelemetry instruments recorder.
 */
class VidLabelingMonitor(
  private val rawImpressionUploadStub:
    RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub,
  private val rawImpressionUploadModelLineStub:
    RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub,
  private val correctionCandidateStub:
    RawImpressionUploadCorrectionCandidateServiceGrpcKt.RawImpressionUploadCorrectionCandidateServiceCoroutineStub,
  private val dispatchSequencer: VidLabelingDispatchSequencer,
  private val dataProviderName: String,
  private val stalenessThreshold: Duration,
  private val rawImpressionsStorageRootUri: String,
  private val rawImpressionsBlobPrefix: String,
  private val rawInputQuietPeriod: Duration,
  private val rawImpressionsExcludedBlobPrefixes: Set<String> = emptySet(),
  rawImpressionsStorageClientProvider: () -> StorageClient,
  private val rawImpressionUploadFileStub:
    RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub,
  vidLabeledImpressionsStorageClientProvider: () -> StorageClient,
  private val poolAssignmentJobStub:
    PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub,
  private val rankerJobStub: RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub,
  private val vidLabelingJobStub: VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub,
  private val workItemsStub: WorkItemsGrpcKt.WorkItemsCoroutineStub,
  private val vidLabeledImpressionsBlobPrefix: String,
  private val rpcThrottlers: VidLabelingRpcThrottlers,
  private val clock: Clock = Clock.systemUTC(),
  private val metrics: VidLabelingMonitorMetrics = VidLabelingMonitorMetrics(),
) {

  // Built lazily so the fast `dispatch` cadence never resolves GCS credentials it does not
  // use; only the `health` data-quality crawl forces these.
  private val rawImpressionsStorageClient: StorageClient by
    lazy(rawImpressionsStorageClientProvider)
  private val rawImpressionInputMonitor: RawImpressionInputMonitor by lazy {
    RawImpressionInputMonitor(
      rawImpressionsStorageClient,
      rawImpressionsStorageRootUri,
      rawImpressionsBlobPrefix,
      rawInputQuietPeriod,
      excludedBlobPrefixes = rawImpressionsExcludedBlobPrefixes,
      clock = clock,
    )
  }
  private val manifestClassifier = RawImpressionUploadManifestClassifier()
  private val vidLabeledImpressionsStorageClient: StorageClient by
    lazy(vidLabeledImpressionsStorageClientProvider)

  /** Outcome of a dispatch-only monitor run (the fast cadence) for a DataProvider. */
  data class DispatchOnlyResult(
    /** Resource name of the upload dispatched this run, or null if none. */
    val dispatchedUpload: String?,
    /** Number of `CREATED` uploads held behind an in-progress upload. */
    val queuedUploads: Int,
    /** Whether the dispatch sequencer threw this run (dispatch is broken for this DataProvider). */
    val dispatchError: Boolean,
  )

  /** Outcome of a health monitor run (the slow cadence) for a DataProvider. */
  data class HealthResult(
    /** Resource names of uploads stuck in a non-terminal state past the SLA. */
    val stuckUploads: List<String>,
    /** Resource names of model lines in `FAILED`. */
    val failedModelLines: List<String>,
    /**
     * `(model line, event date)` pairs under COMPLETED model lines missing their labeled done blob.
     */
    val missingLabeledOutputs: Long,
    /** Files whose create time is after their date folder done blob. */
    val lateArrivingFiles: Long,
    /** Date folders that exist but have no done blob. */
    val missingDoneBlobs: Long,
    /** Date folders whose done blob exists but that hold no data files. */
    val zeroImpressionDates: Long,
    /** Done-marker generations with raw data but no matching upload registration. */
    val unregisteredDoneBlobs: Long,
    /** Parent/child done-marker pairs that claim at least one common raw file. */
    val ambiguousDoneMarkerLayouts: Long,
    /** Registered raw impression files whose blob is absent from storage (data loss). */
    val missingRawFiles: Long,
    /** Whether the non-blocking data-quality crawl failed and its counts are unavailable. */
    val dataQualityCheckFailed: Boolean,
    /** Stuck phase transitions the Monitor re-triggered this run. */
    val recoveredTransitions: Int,
    /** Stuck transitions whose bounded recovery is exhausted (the Monitor has given up; page). */
    val recoveryExhausted: Int,
    /**
     * Stuck transitions that cannot be recovered because the original WorkItem is gone (page); the
     * Monitor stops retrying and a human must intervene.
     */
    val unrecoverableRecoveries: Int,
  ) {
    val hasIssues: Boolean
      get() =
        stuckUploads.isNotEmpty() ||
          failedModelLines.isNotEmpty() ||
          missingLabeledOutputs > 0 ||
          lateArrivingFiles > 0 ||
          missingDoneBlobs > 0 ||
          zeroImpressionDates > 0 ||
          unregisteredDoneBlobs > 0 ||
          ambiguousDoneMarkerLayouts > 0 ||
          missingRawFiles > 0 ||
          dataQualityCheckFailed ||
          recoveryExhausted > 0 ||
          unrecoverableRecoveries > 0
  }

  /**
   * Runs the fast dispatch cadence: delegates to the shared sequencer to start the oldest queued
   * upload for this DataProvider. Does not run any health check.
   */
  suspend fun runDispatch(): DispatchOnlyResult =
    Tracing.traceSuspending(
      spanName = "edpa.vid_labeling.monitor.dispatch",
      attributes = monitorAttributes("monitor_dispatch"),
    ) {
      runDispatchInternal().also { result ->
        Span.current()
          .setAttribute(
            XmmTraceAttributes.OUTCOME,
            if (result.dispatchError) "failed" else "succeeded",
          )
      }
    }

  private suspend fun runDispatchInternal(): DispatchOnlyResult {
    val dispatch: VidLabelingDispatchSequencer.DispatchResult =
      try {
        dispatchSequencer.dispatchNext()
      } catch (e: CancellationException) {
        throw e
      } catch (e: Exception) {
        // The sequencer wraps RPC failures (list calls, model-repo unavailable, non-ALREADY_EXISTS
        // creates) as plain exceptions. Surface them as a metric so operators can tell "dispatch
        // broken" apart from "no work to do".
        metrics.dispatchErrorsGauge.set(1, dataProviderAttributes())
        logger.log(Level.SEVERE, "Dispatch failed for $dataProviderName", e)
        return DispatchOnlyResult(dispatchedUpload = null, queuedUploads = 0, dispatchError = true)
      }
    metrics.dispatchErrorsGauge.set(0, dataProviderAttributes())

    if (dispatch.dispatchedUpload != null) {
      metrics.uploadsDispatchedCounter.add(1, dataProviderAttributes())
      logger.info("Dispatched ${dispatch.dispatchedUpload}")
    }
    metrics.uploadsQueuedGauge.set(dispatch.queuedUploads.toLong(), dataProviderAttributes())
    return DispatchOnlyResult(
      dispatchedUpload = dispatch.dispatchedUpload,
      queuedUploads = dispatch.queuedUploads,
      dispatchError = false,
    )
  }

  /**
   * Runs the slow health cadence: staleness/failure monitoring, stuck-phase recovery, and
   * data-quality checks over a single snapshot of this DataProvider's uploads and model lines. Does
   * not dispatch.
   */
  suspend fun runHealth(): HealthResult =
    Tracing.traceSuspending(
      spanName = "edpa.vid_labeling.monitor.health",
      attributes = monitorAttributes("monitor_health"),
    ) {
      runHealthInternal().also { result ->
        val outcome =
          when {
            result.dataQualityCheckFailed -> "failed"
            result.recoveredTransitions > 0 -> "recovered"
            else -> "succeeded"
          }
        Span.current().setAttribute(XmmTraceAttributes.OUTCOME, outcome)
      }
    }

  private suspend fun runHealthInternal(): HealthResult {
    val snapshot = RunSnapshot(listAllUploads().groupBy { it.state })
    val (stuckUploads, failedModelLines) = checkFailuresAndStaleness(snapshot)
    val recovery = recoverStuckPhases(snapshot)
    val dataQuality = checkDataQuality(snapshot)
    return HealthResult(
      stuckUploads = stuckUploads,
      failedModelLines = failedModelLines,
      missingLabeledOutputs = dataQuality.missingLabeledOutputs,
      lateArrivingFiles = dataQuality.lateArrivingFiles,
      missingDoneBlobs = dataQuality.missingDoneBlobs,
      zeroImpressionDates = dataQuality.zeroImpressionDates,
      unregisteredDoneBlobs = dataQuality.unregisteredDoneBlobs,
      ambiguousDoneMarkerLayouts = dataQuality.ambiguousDoneMarkerLayouts,
      missingRawFiles = dataQuality.missingRawFiles,
      dataQualityCheckFailed = dataQuality.checkFailed,
      recoveredTransitions = recovery.recovered,
      recoveryExhausted = recovery.exhausted,
      unrecoverableRecoveries = recovery.unrecoverable,
    )
  }

  /**
   * Surfaces uploads stuck in a non-terminal state past [stalenessThreshold] (via
   * [VidLabelingMonitorMetrics.uploadsStuckGauge]) and uploads with a `FAILED` model line (via
   * [VidLabelingMonitorMetrics.failedUploadsGauge]) for alerting.
   *
   * @return stuck upload names and failed model line names.
   */
  private suspend fun checkFailuresAndStaleness(
    snapshot: RunSnapshot
  ): Pair<List<String>, List<String>> {
    // CREATED (not yet activated by the sequencer) and ACTIVE (in flight) are the non-terminal
    // states eligible for the staleness check. FAILED is terminal but is still scanned below so a
    // rolled-up FAILED upload's model lines surface.
    val createdUploads: List<RawImpressionUpload> =
      snapshot.uploads(RawImpressionUpload.State.CREATED).filterNot { it.processingDeferred }
    val activeUploads: List<RawImpressionUpload> =
      snapshot.uploads(RawImpressionUpload.State.ACTIVE)
    val failedUploads: List<RawImpressionUpload> =
      snapshot.uploads(RawImpressionUpload.State.FAILED)
    val nowNanos: Long = Timestamps.toNanos(Timestamps.fromMillis(clock.millis()))
    val thresholdNanos: Long = stalenessThreshold.toNanos()

    val stuckUploads: List<String> =
      (createdUploads + activeUploads)
        .filter { nowNanos - Timestamps.toNanos(it.createTime) > thresholdNanos }
        .map { it.name }
    // uploadsStuckGauge is the alerting signal (alert on > 0 over a duration window, which dedupes
    // naturally). Set unconditionally (including 0) so a recovered DataProvider reads back to 0. No
    // per-tick SEVERE log, which would re-page on every Cloud Scheduler tick until manual recovery.
    metrics.uploadsStuckGauge.set(stuckUploads.size.toLong(), dataProviderAttributes())

    // Scan CREATED/ACTIVE/FAILED uploads for FAILED model lines: a rolled-up FAILED upload's lines
    // must surface, as must a FAILED line under an otherwise-live upload. Model lines come from the
    // shared snapshot, so each upload is listed at most once per tick (reused by
    // recoverStuckPhases).
    val failedLinesByUpload: Map<String, List<String>> =
      (createdUploads + activeUploads + failedUploads)
        .associate { upload ->
          upload.name to
            snapshot
              .modelLines(upload.name)
              .filter { it.state == RawImpressionUploadModelLine.State.FAILED }
              .map { it.name }
        }
        .filterValues { it.isNotEmpty() }
    val failedModelLines: List<String> = failedLinesByUpload.values.flatten()
    // failedUploadsGauge unit is {upload}: count uploads with >=1 FAILED model line, not the
    // model-line count (two FAILED lines on one upload is still one failed upload). It is the
    // alerting signal (alert on > 0 over a duration window, which dedupes naturally); no per-tick
    // SEVERE log, which would re-page on every Cloud Scheduler tick until manual recovery. Set
    // unconditionally (including 0) so a recovered DataProvider reads back to 0.
    metrics.failedUploadsGauge.set(failedLinesByUpload.size.toLong(), dataProviderAttributes())

    return stuckUploads to failedModelLines
  }

  /**
   * One tick's view of this DataProvider's uploads (fetched with a single [listAllUploads] and
   * grouped by state) plus a per-upload model-line cache.
   */
  private inner class RunSnapshot(
    private val uploadsByState: Map<RawImpressionUpload.State, List<RawImpressionUpload>>
  ) {
    private val uploadsByName = uploadsByState.values.flatten().associateBy { it.name }
    private val modelLinesByUpload = mutableMapOf<String, List<RawImpressionUploadModelLine>>()

    /** Uploads in [state] (empty if none). */
    fun uploads(state: RawImpressionUpload.State): List<RawImpressionUpload> =
      uploadsByState[state].orEmpty().filter {
        !it.processingDeferred && it.state != RawImpressionUpload.State.CORRECTION_REQUIRED
      }

    /** Every non-deferred, non-correction upload across all states. */
    fun allUploads(): List<RawImpressionUpload> =
      everyUpload().filter {
        !it.processingDeferred && it.state != RawImpressionUpload.State.CORRECTION_REQUIRED
      }

    /** Every upload, including deferred and correction-required revisions. */
    fun everyUpload(): List<RawImpressionUpload> = uploadsByState.values.flatten()

    /** The upload named [name], if it is present in this tick's complete snapshot. */
    fun upload(name: String): RawImpressionUpload? = uploadsByName[name]

    /** [uploadName]'s model lines, listed once per tick and memoized. */
    suspend fun modelLines(uploadName: String): List<RawImpressionUploadModelLine> =
      modelLinesByUpload.getOrPut(uploadName) { listUploadModelLines(uploadName) }
  }

  /**
   * Lists every one of this DataProvider's uploads in one paginated traversal. An empty `state_in`
   * matches all states, so one traversal per tick covers every state; [RunSnapshot] groups the
   * result in memory.
   */
  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private suspend fun listAllUploads(): List<RawImpressionUpload> =
    rawImpressionUploadStub
      .listResources { pageToken: String ->
        // Let StatusException propagate with its gRPC Status.Code intact (this monitor is not a
        // gRPC
        // server, so there is no risk of incorrectly propagating a server status) — callers can
        // then
        // distinguish UNAVAILABLE/NOT_FOUND/PERMISSION_DENIED instead of seeing an opaque
        // Exception.
        val response =
          rpcThrottlers.metadataRead.onReady {
            rawImpressionUploadStub.listRawImpressionUploads(
              listRawImpressionUploadsRequest {
                parent = dataProviderName
                if (pageToken.isNotEmpty()) {
                  this.pageToken = pageToken
                }
              }
            )
          }
        ResourceList(response.rawImpressionUploadsList, response.nextPageToken)
      }
      .flattenConcat()
      .toList()

  /** Lists the `RawImpressionUploadModelLine` children of [uploadName]. */
  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private suspend fun listUploadModelLines(uploadName: String): List<RawImpressionUploadModelLine> =
    rawImpressionUploadModelLineStub
      .listResources { pageToken: String ->
        // Let StatusException propagate with its gRPC Status.Code intact (see listUploads).
        val response =
          rpcThrottlers.metadataRead.onReady {
            rawImpressionUploadModelLineStub.listRawImpressionUploadModelLines(
              listRawImpressionUploadModelLinesRequest {
                parent = uploadName
                if (pageToken.isNotEmpty()) {
                  this.pageToken = pageToken
                }
              }
            )
          }
        ResourceList(response.rawImpressionUploadModelLinesList, response.nextPageToken)
      }
      .flattenConcat()
      .toList()

  /** Streams the `RawImpressionUploadFile` children of [uploadName]. */
  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private fun streamUploadFiles(uploadName: String): Flow<RawImpressionUploadFile> =
    rawImpressionUploadFileStub
      .listResources { pageToken: String ->
        val response =
          rpcThrottlers.metadataRead.onReady {
            rawImpressionUploadFileStub.listRawImpressionUploadFiles(
              listRawImpressionUploadFilesRequest {
                parent = uploadName
                pageSize = RAW_IMPRESSION_UPLOAD_FILE_PAGE_SIZE
                if (pageToken.isNotEmpty()) {
                  this.pageToken = pageToken
                }
              }
            )
          }
        ResourceList(response.rawImpressionUploadFilesList, response.nextPageToken)
      }
      .flattenConcat()

  /** Lists the `RawImpressionUploadFile` children of [uploadName]. */
  private suspend fun listUploadFiles(uploadName: String): List<RawImpressionUploadFile> =
    streamUploadFiles(uploadName).toList()

  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private suspend fun listNoReplacementCorrectionUploadNames(): Set<String> =
    correctionCandidateStub
      .listResources { pageToken: String ->
        val response =
          rpcThrottlers.metadataRead.onReady {
            correctionCandidateStub.listRawImpressionUploadCorrectionCandidates(
              listRawImpressionUploadCorrectionCandidatesRequest {
                parent = dataProviderName
                pageSize = 100
                filter =
                  ListRawImpressionUploadCorrectionCandidatesRequestKt.filter {
                    stateIn += RawImpressionUploadCorrectionCandidate.State.COMPLETE
                  }
                if (pageToken.isNotEmpty()) {
                  this.pageToken = pageToken
                }
              }
            )
          }
        ResourceList(response.rawImpressionUploadCorrectionCandidatesList, response.nextPageToken)
      }
      .flattenConcat()
      .toList()
      .filter {
        it.decision ==
          RawImpressionUploadCorrectionCandidate.Decision.DECISION_REMOVE_WITHOUT_REPLACEMENT
      }
      .mapTo(mutableSetOf()) { candidate -> candidate.rawImpressionUpload }

  /** Returns durable no-replacement manifest boundaries after candidate cleanup. */
  private fun listDurableNoReplacementUploadNames(snapshot: RunSnapshot): Set<String> =
    snapshot.uploads(RawImpressionUpload.State.REMOVED_WITHOUT_REPLACEMENT).mapTo(mutableSetOf()) {
      it.name
    }

  /** Alert-only data-quality signals gathered this run. */
  private data class DataQualityResult(
    val missingLabeledOutputs: Long,
    val lateArrivingFiles: Long,
    val missingDoneBlobs: Long,
    val zeroImpressionDates: Long,
    val unregisteredDoneBlobs: Long,
    val ambiguousDoneMarkerLayouts: Long,
    val missingRawFiles: Long,
    val checkFailed: Boolean,
  )

  private data class RegisteredFileSummary(
    val blobKeys: MutableSet<String>,
    val eventDatesByCompletedUpload: Map<String, Set<LocalDate>>,
    val uploadNamesWithFiles: Set<String>,
  )

  /**
   * Runs the alert-only data-quality checks and sets their gauges (every run, including 0, so a
   * recovered DataProvider reads back to 0). These MUST NOT block dispatch or recovery, so any
   * failure is logged at SEVERE and swallowed -- a bad crawl never fails the monitor run. On a
   * failure the per-signal gauges keep their prior (stale) values, so the data-quality-check-failed
   * gauge is set to 1 to mark them untrustworthy this run (and back to 0 once a crawl completes).
   */
  private suspend fun checkDataQuality(snapshot: RunSnapshot): DataQualityResult {
    return try {
      val noReplacementUploadNames =
        listNoReplacementCorrectionUploadNames() + listDurableNoReplacementUploadNames(snapshot)
      val registeredFiles = summarizeRegisteredFiles(snapshot, noReplacementUploadNames)
      val rawInput =
        rawImpressionInputMonitor.scan(
          missingRegisteredBlobKeys = registeredFiles.blobKeys,
          registeredDoneObjects =
            snapshot
              .everyUpload()
              .filter {
                it.doneBlobGeneration > 0L &&
                  (it.registrationComplete || it.state != RawImpressionUpload.State.CREATED)
              }
              .mapTo(mutableSetOf()) { upload ->
                RawImpressionInputMonitor.DoneObjectIdentity(
                  upload.doneBlobUri,
                  upload.doneBlobGeneration,
                )
              },
          ignoredEmptyDoneObjects =
            snapshot
              .everyUpload()
              .filter {
                it.doneBlobGeneration > 0L &&
                  (it.name in noReplacementUploadNames ||
                    (it.state == RawImpressionUpload.State.CORRECTION_REQUIRED &&
                      it.name !in registeredFiles.uploadNamesWithFiles))
              }
              .mapTo(mutableSetOf()) { upload ->
                RawImpressionInputMonitor.DoneObjectIdentity(
                  upload.doneBlobUri,
                  upload.doneBlobGeneration,
                )
              },
          isNoOpDoneObject = { identity, createTime ->
            isNoOpDoneObject(snapshot, identity, createTime)
          },
        )
      val missingLabeled =
        checkLabelingCompleteness(snapshot, registeredFiles.eventDatesByCompletedUpload)
      val attrs = dataProviderAttributes()
      metrics.missingLabeledOutputsGauge.set(missingLabeled, attrs)
      metrics.missingDoneBlobsGauge.set(rawInput.missingDoneDirectories, attrs)
      metrics.zeroImpressionDatesGauge.set(rawInput.doneWithoutDataDirectories, attrs)
      metrics.unregisteredDoneBlobsGauge.set(rawInput.unregisteredDoneDirectories, attrs)
      metrics.lateArrivingFilesGauge.set(rawInput.dataFilesAfterDone, attrs)
      metrics.ambiguousDoneMarkerLayoutsGauge.set(rawInput.ambiguousDoneLayouts, attrs)
      metrics.missingRawFilesGauge.set(rawInput.missingRegisteredFiles, attrs)
      for (finding in rawInput.findings) {
        VidLabelingTraceLogging.log(
          logger,
          Level.WARNING,
          "edpa.vid_labeling.monitor.raw_input_finding",
          VidLabelingTraceAttributes.DATA_PROVIDER_NAME_STRING to dataProviderName,
          VidLabelingTraceAttributes.GCS_OBJECT_PATH_HASH_STRING to finding.pathHash,
          XmmTraceAttributes.OUTCOME_STRING to finding.type.telemetryValue,
        )
      }
      // The crawl completed, so the gauges above are fresh and trustworthy this run.
      metrics.dataQualityCheckFailedGauge.set(0, attrs)
      DataQualityResult(
        missingLabeledOutputs = missingLabeled,
        lateArrivingFiles = rawInput.dataFilesAfterDone,
        missingDoneBlobs = rawInput.missingDoneDirectories,
        zeroImpressionDates = rawInput.doneWithoutDataDirectories,
        unregisteredDoneBlobs = rawInput.unregisteredDoneDirectories,
        ambiguousDoneMarkerLayouts = rawInput.ambiguousDoneLayouts,
        missingRawFiles = rawInput.missingRegisteredFiles,
        checkFailed = false,
      )
    } catch (e: CancellationException) {
      throw e
    } catch (e: Exception) {
      // Non-blocking: a bad crawl never fails the monitor run. The per-signal gauges above now hold
      // stale values from a previous run, so raise dataQualityCheckFailed=1 (an alertable signal)
      // and log at SEVERE so operators know those numbers are not trustworthy this run.
      metrics.dataQualityCheckFailedGauge.set(1, dataProviderAttributes())
      logger.log(
        Level.SEVERE,
        "Data-quality checks failed for $dataProviderName; gauges may be stale (non-blocking)",
        e,
      )
      VidLabelingTraceLogging.log(
        logger,
        Level.SEVERE,
        "edpa.vid_labeling.monitor.data_quality_failed",
        VidLabelingTraceAttributes.DATA_PROVIDER_NAME_STRING to dataProviderName,
        XmmTraceAttributes.LIFECYCLE_STAGE_STRING to "monitor_health",
        XmmTraceAttributes.OUTCOME_STRING to "failed",
        XmmTraceAttributes.ERROR_TYPE_STRING to XmmTraceAttributes.errorType(e),
        XmmTraceAttributes.ERROR_CODE_STRING to XmmTraceAttributes.errorCode(e),
      )
      DataQualityResult(0L, 0L, 0L, 0L, 0L, 0L, 0L, checkFailed = true)
    }
  }

  private suspend fun isNoOpDoneObject(
    snapshot: RunSnapshot,
    identity: RawImpressionInputMonitor.DoneObjectIdentity,
    createTime: Instant,
  ): Boolean {
    val revisions =
      snapshot.everyUpload().filter { upload -> upload.doneBlobUri == identity.blobUri }
    if (
      revisions.isEmpty() ||
        revisions.any { upload -> upload.doneBlobGeneration == identity.generation }
    ) {
      return false
    }
    val latestRevision = findLatestRevision(revisions)
    val latestRegisteredCreateTime =
      if (latestRevision.hasDoneBlobCreateTime()) {
        latestRevision.doneBlobCreateTime.toInstant()
      } else {
        latestRevision.createTime.toInstant()
      }
    if (
      latestRevision.state == RawImpressionUpload.State.FAILED ||
        !createTime.isAfter(latestRegisteredCreateTime)
    ) {
      return false
    }

    val doneBlobUri = identity.blobUri
    val parsedDoneBlobUri = SelectedStorageClient.parseBlobUri(identity.blobUri)
    val bucket = checkNotNull(parsedDoneBlobUri.bucket)
    val directory = parsedDoneBlobUri.key.substringBeforeLast('/', missingDelimiterValue = "")
    val listingPrefix = if (directory.isEmpty()) "" else "$directory/"
    val blobs =
      rawImpressionsStorageClient
        .listBlobs(listingPrefix)
        .filter { blob ->
          !blob.blobKey
            .substringAfterLast('/')
            .equals(RAW_INPUT_DONE_FILE_NAME, ignoreCase = true) &&
            !isExcludedRawBlobKey(blob.blobKey)
        }
        .toList()
    val currentFiles =
      blobs.map { blob ->
        val generation =
          (blob as? ConditionalOperationStorageClient.Blob)?.freshnessToken?.toLongOrNull()
            ?: return false
        RawImpressionUploadManifestClassifier.File("gs://$bucket/${blob.blobKey}", generation)
      }
    val currentName = "$dataProviderName/rawImpressionUploads/monitor-${identity.generation}"
    val history = revisions.map { upload -> upload.toManifestRevision() }
    val current =
      RawImpressionUploadManifestClassifier.Revision(
        rawImpressionUpload = currentName,
        doneBlobUri = doneBlobUri,
        doneBlobGeneration = identity.generation,
        doneBlobCreateTime = createTime,
        createTime = createTime,
        files = currentFiles,
      )
    return manifestClassifier.classify(currentName, history + current).classification ==
      RawImpressionUploadManifestClassifier.Classification.NO_OP
  }

  private suspend fun RawImpressionUpload.toManifestRevision():
    RawImpressionUploadManifestClassifier.Revision =
    RawImpressionUploadManifestClassifier.Revision(
      rawImpressionUpload = name,
      doneBlobUri = doneBlobUri,
      doneBlobGeneration = doneBlobGeneration,
      doneBlobCreateTime = if (hasDoneBlobCreateTime()) doneBlobCreateTime.toInstant() else null,
      createTime = createTime.toInstant(),
      replacesRawImpressionUpload = replacesRawImpressionUpload,
      uploadHealingOperation = uploadHealingOperation,
      registrationComplete = registrationComplete,
      failed = state == RawImpressionUpload.State.FAILED,
      quarantined = state == RawImpressionUpload.State.CORRECTION_REQUIRED,
      files =
        listUploadFiles(name).map { file ->
          RawImpressionUploadManifestClassifier.File(
            file.blobUri,
            file.blobGeneration,
            file.eventDate,
          )
        },
    )

  private fun findLatestRevision(revisions: List<RawImpressionUpload>): RawImpressionUpload {
    val timestamped = revisions.filter { it.hasDoneBlobCreateTime() }
    if (timestamped.isEmpty()) {
      return revisions.maxWith { left, right ->
        Timestamps.compare(left.createTime, right.createTime)
      }
    }
    return timestamped.maxWith { left, right ->
      val doneTime = Timestamps.compare(left.doneBlobCreateTime, right.doneBlobCreateTime)
      if (doneTime != 0) {
        doneTime
      } else {
        val createTime = Timestamps.compare(left.createTime, right.createTime)
        if (createTime != 0) createTime else left.name.compareTo(right.name)
      }
    }
  }

  private fun isExcludedRawBlobKey(blobKey: String): Boolean =
    rawImpressionsExcludedBlobPrefixes.any { excludedPrefix ->
      val normalized = excludedPrefix.trim('/')
      blobKey == normalized || blobKey.startsWith("$normalized/")
    }

  /** Reads each registered file once without retaining the file protos for the full health run. */
  private suspend fun summarizeRegisteredFiles(
    snapshot: RunSnapshot,
    noReplacementUploadNames: Set<String>,
  ): RegisteredFileSummary {
    val completedUploadNames =
      snapshot.uploads(RawImpressionUpload.State.COMPLETED).mapTo(mutableSetOf()) { upload ->
        upload.name
      }
    val blobKeys = mutableSetOf<String>()
    val eventDatesByCompletedUpload = mutableMapOf<String, MutableSet<LocalDate>>()
    val uploadNamesWithFiles = mutableSetOf<String>()
    streamUploadFiles("$dataProviderName/rawImpressionUploads/-").collect { file ->
      val uploadName = file.name.substringBeforeLast("/files/", missingDelimiterValue = "")
      if (uploadName !in noReplacementUploadNames) {
        blobKeys += SelectedStorageClient.parseBlobUri(file.blobUri).key
      }
      if (uploadName.isNotEmpty()) {
        uploadNamesWithFiles += uploadName
      }
      if (uploadName in completedUploadNames && file.hasEventDate()) {
        eventDatesByCompletedUpload.getOrPut(uploadName) { mutableSetOf() } +=
          LocalDate.of(file.eventDate.year, file.eventDate.month, file.eventDate.day)
      }
    }
    return RegisteredFileSummary(blobKeys, eventDatesByCompletedUpload, uploadNamesWithFiles)
  }

  /**
   * Counts, across COMPLETED uploads and their COMPLETED model lines, the `(model line, event
   * date)` pairs missing their labeled `done` blob at `model-line/<modelLineId>/<event_date>/done`.
   *
   * The event dates come from the upload's registered `RawImpressionUploadFile`s (`event_date`,
   * populated at registration from the file's plaintext footer). At last-job-out the labeler writes
   * that `done` blob for the input event date whether or not any impression survived filtering, so
   * a legitimately all-dropped date still has its `done` blob (no false positive) while a genuinely
   * unfinalized (model line, date) is flagged.
   */
  private suspend fun checkLabelingCompleteness(
    snapshot: RunSnapshot,
    eventDatesByCompletedUpload: Map<String, Set<LocalDate>>,
  ): Long {
    var missing = 0L
    for (upload in snapshot.uploads(RawImpressionUpload.State.COMPLETED)) {
      val eventDates = eventDatesByCompletedUpload[upload.name].orEmpty()
      if (eventDates.isEmpty()) {
        continue
      }
      val completedModelLines =
        snapshot.modelLines(upload.name).filter {
          it.state == RawImpressionUploadModelLine.State.COMPLETED
        }
      for (modelLine in completedModelLines) {
        val modelLineId = ModelLineKey.fromName(modelLine.cmmsModelLine)?.modelLineId ?: continue
        for (eventDate in eventDates) {
          val doneKey = "model-line/$modelLineId/$eventDate/done"
          if (vidLabeledImpressionsStorageClient.getBlob(doneKey) == null) {
            missing++
          }
        }
      }
    }
    return missing
  }

  /** Outcome of a single stuck-transition recovery attempt. */
  private enum class RecoveryOutcome {
    /** A new recovery WorkItem was published this tick. */
    RECOVERED,
    /** All [MAX_RECOVERY_ATTEMPTS] recovery WorkItems already exist; the Monitor has given up. */
    EXHAUSTED,
    /**
     * The original WorkItem to clone is gone (e.g. retention-deleted); recovery is impossible and a
     * human must intervene. Distinct from [NOOP]: retrying never resolves it.
     */
    UNRECOVERABLE,
    /** Nothing to do (not stuck, precondition unmet, or a transient failure to retry next tick). */
    NOOP,
  }

  /** Aggregate recovery outcome for one health run. */
  private data class RecoverySummary(
    val recovered: Int,
    val exhausted: Int,
    val unrecoverable: Int,
  )

  /**
   * Re-triggers stuck memoized phase transitions for this DataProvider and returns how many were
   * recovered and how many are exhausted. A `(upload, model line)` whose child jobs are all
   * SUCCEEDED but whose parent state never advanced (a last-out gate failure) is recovered by
   * re-publishing an existing WorkItem for it: the TEE skips the already-SUCCEEDED job and re-runs
   * its idempotent, parent-state-gated last-out, which performs the fan-out / completion. Never
   * marks anything FAILED.
   *
   * Recovery is bounded: each attempt publishes a distinct `-monitor-recovery-<n>` WorkItem (the
   * persisted WorkItem ids are the durable per-(upload, model line, phase) attempt record), up to
   * [MAX_RECOVERY_ATTEMPTS]. Past that the transition is left stuck, the recovery-exhausted gauge
   * is raised as the page signal, and a `SEVERE` line names the transition for the operator. Only a
   * model line stuck longer than [stalenessThreshold] is recovered, so a legitimately in-flight
   * last-out is never raced.
   */
  private suspend fun recoverStuckPhases(snapshot: RunSnapshot): RecoverySummary {
    val nowNanos: Long = Timestamps.toNanos(Timestamps.fromMillis(clock.millis()))
    val thresholdNanos: Long = stalenessThreshold.toNanos()
    var recovered = 0
    var exhausted = 0
    var unrecoverable = 0
    for (upload in snapshot.uploads(RawImpressionUpload.State.ACTIVE)) {
      for (modelLine in snapshot.modelLines(upload.name)) {
        if (nowNanos - Timestamps.toNanos(modelLine.updateTime) <= thresholdNanos) {
          continue
        }
        val outcome =
          when (modelLine.state) {
            RawImpressionUploadModelLine.State.POOL_ASSIGNING ->
              recoverIfAllPoolAssignmentJobsSucceeded(upload.name, modelLine.cmmsModelLine)
            RawImpressionUploadModelLine.State.RANKING ->
              recoverIfAllRankerJobsSucceeded(upload.name, modelLine.cmmsModelLine)
            RawImpressionUploadModelLine.State.LABELING ->
              recoverIfAllVidLabelingJobsSucceeded(upload.name, modelLine.cmmsModelLine)
            RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING ->
              recoverIfAllVidLabelingJobsSucceeded(upload.name, modelLine.cmmsModelLine)
            else -> RecoveryOutcome.NOOP
          }
        when (outcome) {
          RecoveryOutcome.RECOVERED -> {
            recovered++
            metrics.phaseTransitionsRecoveredCounter.add(1, dataProviderAttributes())
          }
          RecoveryOutcome.EXHAUSTED -> {
            exhausted++
            logger.severe(
              "Recovery exhausted for ${upload.name} model line ${modelLine.cmmsModelLine} in " +
                "${modelLine.state} after $MAX_RECOVERY_ATTEMPTS attempts; manual intervention " +
                "required"
            )
          }
          RecoveryOutcome.UNRECOVERABLE -> {
            unrecoverable++
            logger.severe(
              "Recovery impossible for ${upload.name} model line ${modelLine.cmmsModelLine} in " +
                "${modelLine.state}: original WorkItem is gone; manual intervention required"
            )
          }
          RecoveryOutcome.NOOP -> {}
        }
      }
    }
    // Set each run (including 0) so the page signal self-clears once the transition advances.
    metrics.recoveryExhaustedGauge.set(exhausted.toLong(), dataProviderAttributes())
    metrics.recoveryUnrecoverableGauge.set(unrecoverable.toLong(), dataProviderAttributes())
    return RecoverySummary(
      recovered = recovered,
      exhausted = exhausted,
      unrecoverable = unrecoverable,
    )
  }

  /** Re-publishes a successful Phase-0 shard when every shard job has succeeded. */
  private suspend fun recoverIfAllPoolAssignmentJobsSucceeded(
    uploadName: String,
    modelLine: String,
  ): RecoveryOutcome {
    val jobs = listPoolAssignmentJobs(uploadName, modelLine)
    if (jobs.isEmpty() || jobs.any { it.state != PoolAssignmentJob.State.SUCCEEDED }) {
      return RecoveryOutcome.NOOP
    }
    val job = jobs.minBy { it.shardIndex }
    return republishWorkItem(WorkItemIds.forSubpoolAssigner(uploadName, modelLine, job.shardIndex))
  }

  @OptIn(ExperimentalCoroutinesApi::class)
  private suspend fun listPoolAssignmentJobs(
    uploadName: String,
    modelLine: String,
  ): List<PoolAssignmentJob> =
    poolAssignmentJobStub
      .listResources { pageToken: String ->
        val response =
          rpcThrottlers.metadataRead.onReady {
            poolAssignmentJobStub.listPoolAssignmentJobs(
              listPoolAssignmentJobsRequest {
                parent = uploadName
                filter = ListPoolAssignmentJobsRequestKt.filter { cmmsModelLine = modelLine }
                this.pageToken = pageToken
              }
            )
          }
        ResourceList(response.poolAssignmentJobsList, response.nextPageToken)
      }
      .flattenConcat()
      .toList()

  /**
   * O(1) check (List `total_size`) that every `RankerJob` for `(uploadName, modelLine)` is
   * SUCCEEDED; if so, re-publishes one of their WorkItems to advance RANKING.
   */
  private suspend fun recoverIfAllRankerJobsSucceeded(
    uploadName: String,
    modelLine: String,
  ): RecoveryOutcome {
    val total =
      rpcThrottlers.metadataRead
        .onReady {
          rankerJobStub.listRankerJobs(
            listRankerJobsRequest {
              parent = uploadName
              filter = ListRankerJobsRequestKt.filter { cmmsModelLine = modelLine }
              pageSize = 0
            }
          )
        }
        .totalSize
    if (total == 0) {
      return RecoveryOutcome.NOOP
    }
    val nonSucceeded =
      rpcThrottlers.metadataRead
        .onReady {
          rankerJobStub.listRankerJobs(
            listRankerJobsRequest {
              parent = uploadName
              filter =
                ListRankerJobsRequestKt.filter {
                  cmmsModelLine = modelLine
                  stateIn += listOf(RankerJob.State.CREATED, RankerJob.State.FAILED)
                }
              pageSize = 0
            }
          )
        }
        .totalSize
    if (nonSucceeded > 0) {
      return RecoveryOutcome.NOOP
    }
    val job =
      rpcThrottlers.metadataRead
        .onReady {
          rankerJobStub.listRankerJobs(
            listRankerJobsRequest {
              parent = uploadName
              filter =
                ListRankerJobsRequestKt.filter {
                  cmmsModelLine = modelLine
                  stateIn += RankerJob.State.SUCCEEDED
                }
              pageSize = 1
            }
          )
        }
        .rankerJobsList
        .firstOrNull() ?: return RecoveryOutcome.NOOP
    return republishWorkItem(WorkItemIds.forVidRankBuilder(job.name))
  }

  /**
   * Checks (via bounded single-page lists, since `ListVidLabelingJobs` exposes no `total_size`)
   * that every `VidLabelingJob` for `(uploadName, modelLine)` is SUCCEEDED; if so, re-publishes one
   * of their WorkItems so the labeler completes the parent and writes the done blob.
   */
  private suspend fun recoverIfAllVidLabelingJobsSucceeded(
    uploadName: String,
    modelLine: String,
  ): RecoveryOutcome {
    if (vidLabelingJobsInState(uploadName, modelLine, VidLabelingJob.State.CREATED).isNotEmpty()) {
      return RecoveryOutcome.NOOP
    }
    if (vidLabelingJobsInState(uploadName, modelLine, VidLabelingJob.State.FAILED).isNotEmpty()) {
      return RecoveryOutcome.NOOP
    }
    val job =
      vidLabelingJobsInState(uploadName, modelLine, VidLabelingJob.State.SUCCEEDED).firstOrNull()
        ?: return RecoveryOutcome.NOOP
    return republishWorkItem(WorkItemIds.forVidLabeler(job.name))
  }

  private suspend fun vidLabelingJobsInState(
    uploadName: String,
    modelLine: String,
    state: VidLabelingJob.State,
  ): List<VidLabelingJob> =
    rpcThrottlers.metadataRead
      .onReady {
        vidLabelingJobStub.listVidLabelingJobs(
          listVidLabelingJobsRequest {
            parent = uploadName
            filter =
              ListVidLabelingJobsRequestKt.filter {
                cmmsModelLine = modelLine
                this.state = state
              }
            pageSize = 1
          }
        )
      }
      .vidLabelingJobsList

  /**
   * Bounded re-publish of the WorkItem named `workItems/[workItemId]`: clones its queue + params
   * and creates a fresh WorkItem under `[workItemId]-monitor-recovery-<attempt>` (there is no
   * re-enqueue RPC). Each attempt uses a distinct suffix, so a previously-published recovery no
   * longer blocks the next one; the persisted WorkItem rows are the durable per-transition attempt
   * record.
   *
   * Returns [RecoveryOutcome.RECOVERED] when it publishes a new recovery WorkItem this tick;
   * [RecoveryOutcome.EXHAUSTED] when all [MAX_RECOVERY_ATTEMPTS] already exist (the Monitor gives
   * up); [RecoveryOutcome.UNRECOVERABLE] when the original WorkItem is gone (NOT_FOUND), so it can
   * never be cloned and a human must intervene; [RecoveryOutcome.NOOP] when the original fetch or a
   * create fails with a transient error (retried next tick — the attempt is not burned).
   */
  private suspend fun republishWorkItem(workItemId: String): RecoveryOutcome =
    Tracing.traceSuspending(
      spanName = "edpa.vid_labeling.monitor.recover",
      attributes =
        monitorAttributes("monitor_recovery")
          .toBuilder()
          .put(XmmTraceAttributes.WORK_ITEM_NAME, "workItems/$workItemId")
          .build(),
    ) {
      republishWorkItemInternal(workItemId).also { outcome ->
        if (outcome != RecoveryOutcome.NOOP) {
          Span.current().setAttribute(XmmTraceAttributes.OUTCOME, outcome.name.lowercase())
        }
      }
    }

  private suspend fun republishWorkItemInternal(workItemId: String): RecoveryOutcome {
    val existing =
      try {
        rpcThrottlers.controlPlane.onReady {
          workItemsStub.getWorkItem(getWorkItemRequest { name = "workItems/$workItemId" })
        }
      } catch (e: StatusException) {
        if (e.status.code == Status.Code.NOT_FOUND) {
          // The original WorkItem is gone (e.g. retention-deleted); cloning it is impossible and
          // retrying never succeeds, so escalate instead of counting a transient failure.
          logger.warning("Cannot recover: WorkItem $workItemId is gone (NOT_FOUND)")
          XmmTracing.recordFailure(Span.current(), e)
          return RecoveryOutcome.UNRECOVERABLE
        }
        logger.warning("Cannot recover: WorkItem $workItemId unavailable (${e.status.code})")
        XmmTracing.recordFailure(Span.current(), e)
        metrics.recoveryStepFailuresCounter.add(1, recoveryStepAttributes("get_original"))
        return RecoveryOutcome.NOOP
      }
    for (attempt in 1..WorkItemIds.MAX_MONITOR_RECOVERY_ATTEMPTS) {
      val recoveryId = WorkItemIds.forMonitorRecovery(workItemId, attempt)
      try {
        Span.current()
          .setAttribute(VidLabelingTraceAttributes.RECOVERY_WORK_ITEM_NAME, "workItems/$recoveryId")
        Span.current()
          .addEvent(
            "edpa.vid_labeling.monitor.recovery_attempt",
            Attributes.builder()
              .put(VidLabelingTraceAttributes.RECOVERY_WORK_ITEM_NAME, "workItems/$recoveryId")
              .put(XmmTraceAttributes.OUTCOME, "attempted")
              .build(),
          )
        val published =
          try {
            rpcThrottlers.controlPlane.onReady {
              workItemsStub.createWorkItem(
                createWorkItemRequest {
                  this.workItemId = recoveryId
                  workItem = workItem {
                    queue = existing.queue
                    workItemParams = existing.workItemParams
                  }
                }
              )
            }
            true
          } catch (e: StatusException) {
            if (e.status.code != Status.Code.ALREADY_EXISTS) throw e
            Span.current()
              .addEvent(
                "edpa.vid_labeling.monitor.recovery_attempt",
                Attributes.builder()
                  .put(VidLabelingTraceAttributes.RECOVERY_WORK_ITEM_NAME, "workItems/$recoveryId")
                  .put(XmmTraceAttributes.OUTCOME, "already_exists")
                  .build(),
              )
            false
          }
        if (!published) continue
        logger.info(
          "Recovered a stuck transition by re-publishing WorkItem $workItemId (attempt $attempt)"
        )
        VidLabelingTraceLogging.log(
          logger,
          "edpa.vid_labeling.monitor_recovered",
          VidLabelingTraceAttributes.DATA_PROVIDER_NAME_STRING to dataProviderName,
          XmmTraceAttributes.WORK_ITEM_NAME_STRING to "workItems/$workItemId",
          VidLabelingTraceAttributes.RECOVERY_WORK_ITEM_NAME_STRING to "workItems/$recoveryId",
          XmmTraceAttributes.LIFECYCLE_STAGE_STRING to "monitor_recovery",
          XmmTraceAttributes.OUTCOME_STRING to "recovered",
        )
        return RecoveryOutcome.RECOVERED
      } catch (e: StatusException) {
        logger.warning("Recovery publish failed for $recoveryId (${e.status.code})")
        XmmTracing.recordFailure(Span.current(), e)
        metrics.recoveryStepFailuresCounter.add(1, recoveryStepAttributes("publish"))
        return RecoveryOutcome.NOOP
      }
    }
    return RecoveryOutcome.EXHAUSTED
  }

  private fun dataProviderAttributes(): Attributes =
    Attributes.of(VidLabelingMonitorMetrics.DATA_PROVIDER_ATTR, dataProviderName)

  private fun recoveryStepAttributes(step: String): Attributes =
    Attributes.of(
      VidLabelingMonitorMetrics.DATA_PROVIDER_ATTR,
      dataProviderName,
      VidLabelingMonitorMetrics.RECOVERY_STEP_ATTR,
      step,
    )

  private fun monitorAttributes(lifecycleStage: String): Attributes =
    Attributes.builder()
      .put(VidLabelingTraceAttributes.DATA_PROVIDER_NAME, dataProviderName)
      .put(XmmTraceAttributes.LIFECYCLE_STAGE, lifecycleStage)
      .put(XmmTraceAttributes.OUTCOME, "started")
      .build()

  companion object {
    private const val RAW_INPUT_DONE_FILE_NAME = "done"
    private const val RAW_IMPRESSION_UPLOAD_FILE_PAGE_SIZE = 1000
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    /**
     * Maximum number of recovery WorkItems the Monitor publishes for one stuck transition before
     * escalating (raising [VidLabelingMonitorMetrics.recoveryExhaustedGauge] and giving up). With
     * the daily health cadence this is ~one attempt/day, so escalation fires after ~3 days.
     */
    private const val MAX_RECOVERY_ATTEMPTS = WorkItemIds.MAX_MONITOR_RECOVERY_ATTEMPTS
  }
}
