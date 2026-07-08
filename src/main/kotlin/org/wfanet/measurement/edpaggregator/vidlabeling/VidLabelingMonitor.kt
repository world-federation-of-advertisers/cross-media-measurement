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
import java.time.Clock
import java.time.Duration
import java.time.LocalDate
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.flattenConcat
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankerJobsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListVidLabelingJobsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJob
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJob
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.listRankerJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listVidLabelingJobsRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.createWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.getWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem
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
 *   path) call. The sequencer enforces "at most one upload per DataProvider runs at a time" and
 *   starts the Phase-0/Phase-2 work for the oldest `CREATED` upload when none are `ACTIVE`. Keeping
 *   that logic in one place means the sequencing rule lives in exactly one component with one set
 *   of tests.
 * - **Failure + staleness monitoring:** uploads stuck in a non-terminal state past
 *   [stalenessThreshold] are surfaced via [VidLabelingMonitorMetrics.uploadsStuckGauge], and
 *   uploads with a `FAILED` model line via [VidLabelingMonitorMetrics.failedUploadsGauge], for
 *   duration-window alerting (no per-tick `SEVERE`, to avoid re-paging until manual recovery).
 *
 * Phase-transition advancement (`POOL_ASSIGNING → RANKING → LABELING → COMPLETED`) and data-quality
 * checks are added in follow-up PRs (see #3958); the scan is structured so each is an additive
 * step.
 *
 * @param rawImpressionUploadStub stub for `RawImpressionUploadService`.
 * @param rawImpressionUploadModelLineStub stub for `RawImpressionUploadModelLineService`.
 * @param dispatchSequencer shared sequencer that performs dispatch for this DataProvider.
 * @param dataProviderName resource name of the `DataProvider` this monitor scans.
 * @param stalenessThreshold non-terminal uploads older than this are flagged as stuck.
 * @param clock clock used for staleness evaluation.
 * @param metrics OpenTelemetry instruments recorder.
 */
class VidLabelingMonitor(
  private val rawImpressionUploadStub:
    RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub,
  private val rawImpressionUploadModelLineStub:
    RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub,
  private val dispatchSequencer: VidLabelingDispatchSequencer,
  private val dataProviderName: String,
  private val stalenessThreshold: Duration,
  private val rawImpressionsStorageClient: StorageClient,
  private val rawImpressionUploadFileStub:
    RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub,
  private val vidLabeledImpressionsStorageClient: StorageClient,
  private val rankerJobStub: RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub,
  private val vidLabelingJobStub: VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub,
  private val workItemsStub: WorkItemsGrpcKt.WorkItemsCoroutineStub,
  private val clock: Clock = Clock.systemUTC(),
  private val metrics: VidLabelingMonitorMetrics = VidLabelingMonitorMetrics(),
) {

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
    /** Registered raw impression files whose blob is absent from storage (data loss). */
    val missingRawFiles: Long,
    /** Stuck phase transitions the Monitor re-triggered this run. */
    val recoveredTransitions: Int,
    /** Stuck transitions whose bounded recovery is exhausted (the Monitor has given up; page). */
    val recoveryExhausted: Int,
  ) {
    val hasIssues: Boolean
      get() =
        stuckUploads.isNotEmpty() ||
          failedModelLines.isNotEmpty() ||
          missingLabeledOutputs > 0 ||
          lateArrivingFiles > 0 ||
          missingDoneBlobs > 0 ||
          zeroImpressionDates > 0 ||
          missingRawFiles > 0 ||
          recoveredTransitions > 0 ||
          recoveryExhausted > 0
  }

  // TODO(world-federation-of-advertisers/cross-media-measurement#4044): stuck-POOL_ASSIGNING
  //   phase-transition recovery is deferred until the PoolAssignmentJobService implementation
  //   (#4044) is in this branch base. Both the O(1) "all PoolAssignmentJobs SUCCEEDED" detection
  //   (ListPoolAssignmentJobs total_size with a state filter) and end-to-end recovery require
  //   that service, which is not yet in ancestry. Stuck-RANKING/stuck-LABELING recovery and the
  //   data-quality checks do not depend on it.
  /**
   * Runs the fast dispatch cadence: delegates to the shared sequencer to start the oldest queued
   * upload for this DataProvider. Does not run any health check.
   */
  suspend fun runDispatch(): DispatchOnlyResult {
    val dispatch: VidLabelingDispatchSequencer.DispatchResult =
      try {
        dispatchSequencer.dispatchNext()
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
    metrics.uploadsQueuedGauge.set(
      dispatch.queuedUploads.toLong(),
      dataProviderAttributes(),
    )
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
  suspend fun runHealth(): HealthResult {
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
      missingRawFiles = dataQuality.missingRawFiles,
      recoveredTransitions = recovery.recovered,
      recoveryExhausted = recovery.exhausted,
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
      snapshot.uploads(RawImpressionUpload.State.CREATED)
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
    metrics.uploadsStuckGauge.set(
      stuckUploads.size.toLong(),
      dataProviderAttributes(),
    )

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
    metrics.failedUploadsGauge.set(
      failedLinesByUpload.size.toLong(),
      dataProviderAttributes(),
    )

    return stuckUploads to failedModelLines
  }

  /**
   * One tick's view of this DataProvider's uploads (fetched with a single [listAllUploads] and
   * grouped by state) plus a per-upload model-line cache, so each upload's model lines are listed
   * at most once per tick across all checks (1 + N list RPCs per tick instead of one list per
   * check).
   */
  private inner class RunSnapshot(
    private val uploadsByState: Map<RawImpressionUpload.State, List<RawImpressionUpload>>
  ) {
    private val modelLinesByUpload = mutableMapOf<String, List<RawImpressionUploadModelLine>>()

    /** Uploads in [state] (empty if none). */
    fun uploads(state: RawImpressionUpload.State): List<RawImpressionUpload> =
      uploadsByState[state].orEmpty()

    /** Every upload across all states. */
    fun allUploads(): List<RawImpressionUpload> = uploadsByState.values.flatten()

    /** [uploadName]'s model lines, listed once per tick and memoized. */
    suspend fun modelLines(uploadName: String): List<RawImpressionUploadModelLine> =
      modelLinesByUpload.getOrPut(uploadName) { listUploadModelLines(uploadName) }
  }

  /**
   * Lists every one of this DataProvider's uploads in a single RPC. An empty `state_in` matches all
   * states, so one list per tick covers every state; [RunSnapshot] groups the result in memory.
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
          rawImpressionUploadStub.listRawImpressionUploads(
            listRawImpressionUploadsRequest {
              parent = dataProviderName
              if (pageToken.isNotEmpty()) {
                this.pageToken = pageToken
              }
            }
          )
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
          rawImpressionUploadModelLineStub.listRawImpressionUploadModelLines(
            listRawImpressionUploadModelLinesRequest {
              parent = uploadName
              if (pageToken.isNotEmpty()) {
                this.pageToken = pageToken
              }
            }
          )
        ResourceList(response.rawImpressionUploadModelLinesList, response.nextPageToken)
      }
      .flattenConcat()
      .toList()

  /** Lists the `RawImpressionUploadFile` children of [uploadName]. */
  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private suspend fun listUploadFiles(uploadName: String): List<RawImpressionUploadFile> =
    rawImpressionUploadFileStub
      .listResources { pageToken: String ->
        val response =
          rawImpressionUploadFileStub.listRawImpressionUploadFiles(
            listRawImpressionUploadFilesRequest {
              parent = uploadName
              if (pageToken.isNotEmpty()) {
                this.pageToken = pageToken
              }
            }
          )
        ResourceList(response.rawImpressionUploadFilesList, response.nextPageToken)
      }
      .flattenConcat()
      .toList()

  /** Alert-only data-quality signals gathered this run. */
  private data class DataQualityResult(
    val missingLabeledOutputs: Long,
    val lateArrivingFiles: Long,
    val missingDoneBlobs: Long,
    val zeroImpressionDates: Long,
    val missingRawFiles: Long,
  )

  /** Raw-impression storage-crawl signals (a subset of [DataQualityResult]). */
  private data class StorageQuality(
    val missingRawFiles: Long,
    val missingDoneBlobs: Long,
    val zeroImpressionDates: Long,
    val lateArrivingFiles: Long,
  )

  /**
   * Runs the alert-only data-quality checks and sets their gauges (every run, including 0, so a
   * recovered DataProvider reads back to 0). These MUST NOT block dispatch or recovery, so any
   * failure is logged at WARNING and swallowed -- a bad crawl never fails the monitor run.
   */
  private suspend fun checkDataQuality(snapshot: RunSnapshot): DataQualityResult {
    return try {
      val storage = checkRawImpressionStorageQuality(snapshot)
      val missingLabeled = checkLabelingCompleteness(snapshot)
      val attrs = dataProviderAttributes()
      metrics.missingLabeledOutputsGauge.set(missingLabeled, attrs)
      metrics.missingDoneBlobsGauge.set(storage.missingDoneBlobs, attrs)
      metrics.zeroImpressionDatesGauge.set(storage.zeroImpressionDates, attrs)
      metrics.lateArrivingFilesGauge.set(storage.lateArrivingFiles, attrs)
      metrics.missingRawFilesGauge.set(storage.missingRawFiles, attrs)
      DataQualityResult(
        missingLabeledOutputs = missingLabeled,
        lateArrivingFiles = storage.lateArrivingFiles,
        missingDoneBlobs = storage.missingDoneBlobs,
        zeroImpressionDates = storage.zeroImpressionDates,
        missingRawFiles = storage.missingRawFiles,
      )
    } catch (e: Exception) {
      logger.log(
        Level.WARNING,
        "Data-quality checks failed for $dataProviderName (non-blocking)",
        e,
      )
      DataQualityResult(0L, 0L, 0L, 0L, 0L)
    }
  }

  /**
   * Reconciles registered raw-impression files (the metadata source of truth) against raw storage,
   * keyed off each file's `event_date`. Reports (a) `missing_raw_files`: a file registered in
   * metadata whose blob is absent from storage (data loss -- e.g. retention deleted it); (b)
   * `missing_done_blobs`: an event-date folder with registered files but no `done` blob; (c)
   * `zero_impression_dates`: a folder whose `done` blob exists but that holds no data files; and
   * (d) `late_arriving_files`: files whose storage create time is after the folder's `done` blob.
   * The date folders come from metadata (`event_date` + `blob_uri`), not a string-parsed
   * `done_blob_uri`, so coverage follows what was registered and metadata-vs-storage divergence is
   * detectable.
   */
  private suspend fun checkRawImpressionStorageQuality(snapshot: RunSnapshot): StorageQuality {
    // Registered files are the source of truth: each carries its event_date (from the footer) and
    // blob_uri (its storage location).
    data class DatedFile(val key: String, val eventDate: LocalDate)
    val files: List<DatedFile> =
      snapshot
        .allUploads()
        .flatMap { upload -> listUploadFiles(upload.name) }
        .filter { it.hasEventDate() }
        .map { file ->
          DatedFile(
            SelectedStorageClient.parseBlobUri(file.blobUri).key,
            LocalDate.of(file.eventDate.year, file.eventDate.month, file.eventDate.day),
          )
        }

    // (data loss) A file registered in metadata whose blob is no longer in storage.
    var missingRawFiles = 0L
    for (file in files) {
      if (rawImpressionsStorageClient.getBlob(file.key) == null) {
        missingRawFiles++
      }
    }

    // Reconcile each event-date folder. The folder's trailing segment must equal the file's
    // event_date, so a file stored under a path that disagrees with its footer date is not counted
    // as a spurious date folder.
    val dateFolders: Set<String> =
      files
        .mapNotNull { file ->
          val folder = file.key.substringBeforeLast('/', "")
          if (folder.substringAfterLast('/') == file.eventDate.toString()) folder else null
        }
        .toSet()

    var missingDoneBlobs = 0L
    var zeroImpressionDates = 0L
    var lateArrivingFiles = 0L
    for (dateFolder in dateFolders) {
      val doneBlob = rawImpressionsStorageClient.getBlob("$dateFolder/done")
      if (doneBlob == null) {
        missingDoneBlobs++
        continue
      }
      val dataFiles =
        rawImpressionsStorageClient.listBlobs("$dateFolder/").toList().filterNot {
          it.blobKey.endsWith("/done")
        }
      if (dataFiles.isEmpty()) {
        zeroImpressionDates++
      }
      lateArrivingFiles += dataFiles.count { it.createTime.isAfter(doneBlob.createTime) }.toLong()
    }
    return StorageQuality(missingRawFiles, missingDoneBlobs, zeroImpressionDates, lateArrivingFiles)
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
  private suspend fun checkLabelingCompleteness(snapshot: RunSnapshot): Long {
    var missing = 0L
    for (upload in snapshot.uploads(RawImpressionUpload.State.COMPLETED)) {
      val eventDates: Set<LocalDate> =
        listUploadFiles(upload.name)
          .filter { it.hasEventDate() }
          .map { LocalDate.of(it.eventDate.year, it.eventDate.month, it.eventDate.day) }
          .toSet()
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
    /** Nothing to do (not stuck, precondition unmet, or a transient failure to retry next tick). */
    NOOP,
  }

  /** Aggregate recovery outcome for one health run. */
  private data class RecoverySummary(val recovered: Int, val exhausted: Int)

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
   * last-out is never raced. Stuck `POOL_ASSIGNING` recovery is deferred to #4044 (see the TODO
   * above [runDispatch]); it needs `PoolAssignmentJobService`, which is not in this base.
   */
  private suspend fun recoverStuckPhases(snapshot: RunSnapshot): RecoverySummary {
    val nowNanos: Long = Timestamps.toNanos(Timestamps.fromMillis(clock.millis()))
    val thresholdNanos: Long = stalenessThreshold.toNanos()
    var recovered = 0
    var exhausted = 0
    for (upload in snapshot.uploads(RawImpressionUpload.State.ACTIVE)) {
      for (modelLine in snapshot.modelLines(upload.name)) {
        if (nowNanos - Timestamps.toNanos(modelLine.updateTime) <= thresholdNanos) {
          continue
        }
        val outcome =
          when (modelLine.state) {
            RawImpressionUploadModelLine.State.RANKING ->
              recoverIfAllRankerJobsSucceeded(upload.name, modelLine.cmmsModelLine)
            RawImpressionUploadModelLine.State.LABELING ->
              recoverIfAllVidLabelingJobsSucceeded(upload.name, modelLine.cmmsModelLine)
            // POOL_ASSIGNING recovery deferred to #4044 (PoolAssignmentJobService not in base).
            else -> RecoveryOutcome.NOOP
          }
        when (outcome) {
          RecoveryOutcome.RECOVERED -> {
            recovered++
            metrics.phaseTransitionsRecoveredCounter.add(
              1,
              dataProviderAttributes(),
            )
          }
          RecoveryOutcome.EXHAUSTED -> {
            exhausted++
            logger.severe(
              "Recovery exhausted for ${upload.name} model line ${modelLine.cmmsModelLine} in " +
                "${modelLine.state} after $MAX_RECOVERY_ATTEMPTS attempts; manual intervention " +
                "required"
            )
          }
          RecoveryOutcome.NOOP -> {}
        }
      }
    }
    // Set each run (including 0) so the page signal self-clears once the transition advances.
    metrics.recoveryExhaustedGauge.set(
      exhausted.toLong(),
      dataProviderAttributes(),
    )
    return RecoverySummary(recovered = recovered, exhausted = exhausted)
  }

  /**
   * O(1) check (List `total_size`) that every `RankerJob` for `(uploadName, modelLine)` is
   * SUCCEEDED; if so, re-publishes one of their WorkItems to advance RANKING.
   */
  private suspend fun recoverIfAllRankerJobsSucceeded(
    uploadName: String,
    modelLine: String,
  ): RecoveryOutcome {
    val total =
      rankerJobStub
        .listRankerJobs(
          listRankerJobsRequest {
            parent = uploadName
            filter = ListRankerJobsRequestKt.filter { cmmsModelLine = modelLine }
            pageSize = 0
          }
        )
        .totalSize
    if (total == 0) {
      return RecoveryOutcome.NOOP
    }
    val nonSucceeded =
      rankerJobStub
        .listRankerJobs(
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
        .totalSize
    if (nonSucceeded > 0) {
      return RecoveryOutcome.NOOP
    }
    val job =
      rankerJobStub
        .listRankerJobs(
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
    vidLabelingJobStub
      .listVidLabelingJobs(
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
   * up); [RecoveryOutcome.NOOP] when the original WorkItem is unavailable or a create fails with a
   * transient error (retried next tick — the attempt is not burned).
   */
  private suspend fun republishWorkItem(workItemId: String): RecoveryOutcome {
    val existing =
      try {
        workItemsStub.getWorkItem(getWorkItemRequest { name = "workItems/$workItemId" })
      } catch (e: StatusException) {
        logger.warning("Cannot recover: WorkItem $workItemId unavailable (${e.status.code})")
        metrics.recoveryStepFailuresCounter.add(
          1,
          recoveryStepAttributes("get_original"),
        )
        return RecoveryOutcome.NOOP
      }
    for (attempt in 1..MAX_RECOVERY_ATTEMPTS) {
      val recoveryId = "$workItemId-monitor-recovery-$attempt"
      try {
        workItemsStub.createWorkItem(
          createWorkItemRequest {
            this.workItemId = recoveryId
            workItem = workItem {
              queue = existing.queue
              workItemParams = existing.workItemParams
            }
          }
        )
        logger.info(
          "Recovered a stuck transition by re-publishing WorkItem $workItemId (attempt $attempt)"
        )
        return RecoveryOutcome.RECOVERED
      } catch (e: StatusException) {
        if (e.status.code == Status.Code.ALREADY_EXISTS) {
          // This attempt was already published on a prior tick; try the next suffix.
          continue
        }
        logger.warning("Recovery publish failed for $recoveryId (${e.status.code})")
        metrics.recoveryStepFailuresCounter.add(
          1,
          recoveryStepAttributes("publish"),
        )
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

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    /**
     * Maximum number of recovery WorkItems the Monitor publishes for one stuck transition before
     * escalating (raising [VidLabelingMonitorMetrics.recoveryExhaustedGauge] and giving up). With
     * the daily health cadence this is ~one attempt/day, so escalation fires after ~3 days.
     */
    private const val MAX_RECOVERY_ATTEMPTS = 3
  }
}
