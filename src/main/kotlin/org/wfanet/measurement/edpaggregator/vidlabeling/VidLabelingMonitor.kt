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
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.flattenConcat
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilityBlobs
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankerJobsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListVidLabelingJobsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJob
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJob
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.listRankerJobsRequest
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
 * [VidLabelingMonitor] and calls [run]. This first iteration (#3958) implements:
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
  private val rankerJobStub: RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub,
  private val vidLabelingJobStub: VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub,
  private val workItemsStub: WorkItemsGrpcKt.WorkItemsCoroutineStub,
  private val clock: Clock = Clock.systemUTC(),
) {

  /** Outcome of one monitor run for a DataProvider. */
  data class MonitorResult(
    /** Resource name of the upload dispatched this run, or null if none. */
    val dispatchedUpload: String?,
    /** Number of `CREATED` uploads held behind an in-progress upload. */
    val queuedUploads: Int,
    /** Resource names of uploads stuck in a non-terminal state past the SLA. */
    val stuckUploads: List<String>,
    /** Resource names of model lines in `FAILED`. */
    val failedModelLines: List<String>,
    /** Whether the dispatch sequencer threw this run (dispatch is broken for this DataProvider). */
    val dispatchError: Boolean,
    /** `VidLabelingJob`s under COMPLETED model lines that did not reach SUCCEEDED. */
    val missingLabeledOutputs: Long,
    /** Files whose create time is after their date folder done blob. */
    val lateArrivingFiles: Long,
    /** Date folders that exist but have no done blob. */
    val missingDoneBlobs: Long,
    /** Date folders whose done blob exists but that hold no data files. */
    val zeroImpressionDates: Long,
    /** Stuck phase transitions the Monitor re-triggered this run. */
    val recoveredTransitions: Int,
  ) {
    val hasIssues: Boolean
      get() =
        dispatchError ||
          stuckUploads.isNotEmpty() ||
          failedModelLines.isNotEmpty() ||
          missingLabeledOutputs > 0 ||
          lateArrivingFiles > 0 ||
          missingDoneBlobs > 0 ||
          zeroImpressionDates > 0 ||
          recoveredTransitions > 0
  }

  // TODO(world-federation-of-advertisers/cross-media-measurement#4044): stuck-POOL_ASSIGNING
  //   phase-transition recovery is deferred until the PoolAssignmentJobService implementation
  //   (#4044) is in this branch base. Both the O(1) "all PoolAssignmentJobs SUCCEEDED" detection
  //   (ListPoolAssignmentJobs total_size with a state filter) and end-to-end recovery require
  //   that service, which is not yet in ancestry. Stuck-RANKING/stuck-LABELING recovery and the
  //   data-quality checks do not depend on it.
  /** Delegates dispatch to the sequencer, then reports health issues for this DataProvider. */
  suspend fun run(): MonitorResult {
    val dispatch: VidLabelingDispatchSequencer.DispatchResult =
      try {
        dispatchSequencer.dispatchNext()
      } catch (e: Exception) {
        // The sequencer wraps RPC failures (list calls, model-repo unavailable, non-ALREADY_EXISTS
        // creates) as plain exceptions. Surface them as a metric so operators can tell "dispatch
        // broken" apart from "no work to do"; the staleness/failure checks below are skipped
        // because
        // they share the same backend that just failed.
        VidLabelingMonitorMetrics.dispatchErrorsGauge.set(1, dataProviderAttributes())
        logger.log(Level.SEVERE, "Dispatch failed for $dataProviderName", e)
        return MonitorResult(
          dispatchedUpload = null,
          queuedUploads = 0,
          stuckUploads = emptyList(),
          failedModelLines = emptyList(),
          dispatchError = true,
          missingLabeledOutputs = 0,
          lateArrivingFiles = 0,
          missingDoneBlobs = 0,
          zeroImpressionDates = 0,
          recoveredTransitions = 0,
        )
      }
    VidLabelingMonitorMetrics.dispatchErrorsGauge.set(0, dataProviderAttributes())

    if (dispatch.dispatchedUpload != null) {
      VidLabelingMonitorMetrics.uploadsDispatchedCounter.add(1, dataProviderAttributes())
      logger.info("Dispatched ${dispatch.dispatchedUpload}")
    }
    VidLabelingMonitorMetrics.uploadsQueuedGauge.set(
      dispatch.queuedUploads.toLong(),
      dataProviderAttributes(),
    )

    val (stuckUploads, failedModelLines) = checkFailuresAndStaleness()
    val recoveredTransitions = recoverStuckPhases()
    val dataQuality = checkDataQuality()
    return MonitorResult(
      dispatchedUpload = dispatch.dispatchedUpload,
      queuedUploads = dispatch.queuedUploads,
      stuckUploads = stuckUploads,
      failedModelLines = failedModelLines,
      dispatchError = false,
      missingLabeledOutputs = dataQuality.missingLabeledOutputs,
      lateArrivingFiles = dataQuality.lateArrivingFiles,
      missingDoneBlobs = dataQuality.missingDoneBlobs,
      zeroImpressionDates = dataQuality.zeroImpressionDates,
      recoveredTransitions = recoveredTransitions,
    )
  }

  /**
   * Surfaces uploads stuck in a non-terminal state past [stalenessThreshold] (via
   * [VidLabelingMonitorMetrics.uploadsStuckGauge]) and uploads with a `FAILED` model line (via
   * [VidLabelingMonitorMetrics.failedUploadsGauge]) for alerting.
   *
   * @return stuck upload names and failed model line names.
   */
  private suspend fun checkFailuresAndStaleness(): Pair<List<String>, List<String>> {
    // CREATED (not yet activated by the sequencer) and ACTIVE (in flight) are the non-terminal
    // states eligible for the staleness check. FAILED is terminal but is still scanned below so a
    // rolled-up FAILED upload's model lines surface.
    val createdUploads: List<RawImpressionUpload> = listUploads(RawImpressionUpload.State.CREATED)
    val activeUploads: List<RawImpressionUpload> = listUploads(RawImpressionUpload.State.ACTIVE)
    val failedUploads: List<RawImpressionUpload> = listUploads(RawImpressionUpload.State.FAILED)
    val nowNanos: Long = Timestamps.toNanos(Timestamps.fromMillis(clock.millis()))
    val thresholdNanos: Long = stalenessThreshold.toNanos()

    val stuckUploads: List<String> =
      (createdUploads + activeUploads)
        .filter { nowNanos - Timestamps.toNanos(it.createTime) > thresholdNanos }
        .map { it.name }
    // uploadsStuckGauge is the alerting signal (alert on > 0 over a duration window, which dedupes
    // naturally). Set unconditionally (including 0) so a recovered DataProvider reads back to 0. No
    // per-tick SEVERE log, which would re-page on every Cloud Scheduler tick until manual recovery.
    VidLabelingMonitorMetrics.uploadsStuckGauge.set(
      stuckUploads.size.toLong(),
      dataProviderAttributes(),
    )

    // Scan CREATED/ACTIVE/FAILED uploads for FAILED model lines: a rolled-up FAILED upload's lines
    // must surface, as must a FAILED line under an otherwise-live upload.
    // N+1: one ListRawImpressionUploadModelLines per upload, bounded by the small number of
    // concurrent uploads per DataProvider (sequencing keeps it low), so acceptable here.
    val failedLinesByUpload: Map<String, List<String>> =
      (createdUploads + activeUploads + failedUploads)
        .associate { upload ->
          upload.name to
            listUploadModelLines(upload.name)
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
    VidLabelingMonitorMetrics.failedUploadsGauge.set(
      failedLinesByUpload.size.toLong(),
      dataProviderAttributes(),
    )

    return stuckUploads to failedModelLines
  }

  /** Lists this DataProvider's uploads in [state]. */
  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private suspend fun listUploads(state: RawImpressionUpload.State): List<RawImpressionUpload> =
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
              filter = ListRawImpressionUploadsRequestKt.filter { stateIn += state }
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

  /** Alert-only data-quality signals gathered this run. */
  private data class DataQualityResult(
    val missingLabeledOutputs: Long,
    val lateArrivingFiles: Long,
    val missingDoneBlobs: Long,
    val zeroImpressionDates: Long,
  )

  /** Raw-impression storage-crawl signals (a subset of [DataQualityResult]). */
  private data class StorageQuality(
    val missingDoneBlobs: Long,
    val zeroImpressionDates: Long,
    val lateArrivingFiles: Long,
  )

  /**
   * Runs the alert-only data-quality checks and sets their gauges (every run, including 0, so a
   * recovered DataProvider reads back to 0). These MUST NOT block dispatch or recovery, so any
   * failure is logged at WARNING and swallowed -- a bad crawl never fails the monitor run.
   */
  private suspend fun checkDataQuality(): DataQualityResult {
    return try {
      val storage = checkRawImpressionStorageQuality()
      val missingLabeled = checkLabelingCompleteness()
      val attrs = dataProviderAttributes()
      VidLabelingMonitorMetrics.missingLabeledOutputsGauge.set(missingLabeled, attrs)
      VidLabelingMonitorMetrics.missingDoneBlobsGauge.set(storage.missingDoneBlobs, attrs)
      VidLabelingMonitorMetrics.zeroImpressionDatesGauge.set(storage.zeroImpressionDates, attrs)
      VidLabelingMonitorMetrics.lateArrivingFilesGauge.set(storage.lateArrivingFiles, attrs)
      DataQualityResult(
        missingLabeledOutputs = missingLabeled,
        lateArrivingFiles = storage.lateArrivingFiles,
        missingDoneBlobs = storage.missingDoneBlobs,
        zeroImpressionDates = storage.zeroImpressionDates,
      )
    } catch (e: Exception) {
      logger.log(
        Level.WARNING,
        "Data-quality checks failed for $dataProviderName (non-blocking)",
        e,
      )
      DataQualityResult(0L, 0L, 0L, 0L)
    }
  }

  /**
   * Crawls the raw-impression date folders and counts (a) date folders with no done blob, (b) date
   * folders whose done blob exists but that hold no data files, and (c) files whose create time is
   * after their date's done blob (written after the EDP marked the date done). The date-folder
   * parent prefix is derived from each upload's own `done_blob_uri` (key `<prefix>/<date>/done`),
   * so the per-EDP path is discovered rather than assumed; non-date folders are skipped by
   * [DataAvailabilityBlobs.enumerateDateInfo].
   */
  private suspend fun checkRawImpressionStorageQuality(): StorageQuality {
    val uploads =
      listUploads(RawImpressionUpload.State.CREATED) +
        listUploads(RawImpressionUpload.State.ACTIVE) +
        listUploads(RawImpressionUpload.State.COMPLETED) +
        listUploads(RawImpressionUpload.State.FAILED)
    val dateFolderPrefixes: Set<String> =
      uploads
        .mapNotNull { upload ->
          val key = SelectedStorageClient.parseBlobUri(upload.doneBlobUri).key
          val dateFolder = key.substringBeforeLast('/', "")
          if (dateFolder.isEmpty()) {
            return@mapNotNull null
          }
          val parent = dateFolder.substringBeforeLast('/', "")
          if (parent.isEmpty()) "" else "$parent/"
        }
        .toSet()

    var missingDoneBlobs = 0L
    var zeroImpressionDates = 0L
    var lateArrivingFiles = 0L
    for (prefix in dateFolderPrefixes) {
      val info = DataAvailabilityBlobs.enumerateDateInfo(rawImpressionsStorageClient, prefix)
      missingDoneBlobs += info.datesWithoutDoneBlob.size.toLong()
      for ((date: LocalDate, doneBlob) in info.datesWithDoneBlob) {
        val dataFiles =
          rawImpressionsStorageClient.listBlobs("$prefix$date/").toList().filterNot {
            it.blobKey.endsWith("/done")
          }
        if (dataFiles.isEmpty()) {
          zeroImpressionDates++
        }
        lateArrivingFiles += dataFiles.count { it.createTime.isAfter(doneBlob.createTime) }.toLong()
      }
    }
    return StorageQuality(missingDoneBlobs, zeroImpressionDates, lateArrivingFiles)
  }

  /**
   * Counts, across COMPLETED uploads and their COMPLETED model lines, the `VidLabelingJob`s that
   * did not reach SUCCEEDED (CREATED = never processed, FAILED = processing failed). The labeler
   * marks a job SUCCEEDED when it finishes processing, whether or not it wrote any output blob, so
   * a model line whose every impression is legitimately dropped (e.g. a backfill whose events fall
   * outside the model line's active window) still SUCCEEDs and is NOT counted. Keying off job state
   * -- rather than a storage-blob census -- avoids the false positive such a model line would
   * otherwise raise on every scheduler tick.
   */
  private suspend fun checkLabelingCompleteness(): Long {
    var missing = 0L
    for (upload in listUploads(RawImpressionUpload.State.COMPLETED)) {
      val completedModelLines =
        listUploadModelLines(upload.name).filter {
          it.state == RawImpressionUploadModelLine.State.COMPLETED
        }
      for (modelLine in completedModelLines) {
        missing +=
          countVidLabelingJobsInState(
            upload.name,
            modelLine.cmmsModelLine,
            VidLabelingJob.State.CREATED,
          )
        missing +=
          countVidLabelingJobsInState(
            upload.name,
            modelLine.cmmsModelLine,
            VidLabelingJob.State.FAILED,
          )
      }
    }
    return missing
  }

  /**
   * Total number of `VidLabelingJob`s for `(uploadName, modelLine)` in [state] (fully paginated).
   */
  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private suspend fun countVidLabelingJobsInState(
    uploadName: String,
    modelLine: String,
    state: VidLabelingJob.State,
  ): Long =
    vidLabelingJobStub
      .listResources { pageToken: String ->
        val response =
          vidLabelingJobStub.listVidLabelingJobs(
            listVidLabelingJobsRequest {
              parent = uploadName
              filter =
                ListVidLabelingJobsRequestKt.filter {
                  cmmsModelLine = modelLine
                  this.state = state
                }
              if (pageToken.isNotEmpty()) {
                this.pageToken = pageToken
              }
            }
          )
        ResourceList(response.vidLabelingJobsList, response.nextPageToken)
      }
      .flattenConcat()
      .toList()
      .size
      .toLong()

  /**
   * Re-triggers stuck memoized phase transitions for this DataProvider and returns how many were
   * recovered. A `(upload, model line)` whose child jobs are all SUCCEEDED but whose parent state
   * never advanced (a last-out gate failure) is recovered by re-publishing ONE existing WorkItem
   * for it: the TEE skips the already-SUCCEEDED job and re-runs its idempotent, parent-state-gated
   * last-out, which performs the fan-out / completion. Never marks anything FAILED.
   *
   * Only a model line stuck in the phase longer than [stalenessThreshold] is recovered, so a
   * legitimately in-flight last-out is never raced. Stuck `POOL_ASSIGNING` recovery is deferred
   * to #4044 (see the TODO above [run]); it needs `PoolAssignmentJobService`, which is not in this
   * base.
   */
  private suspend fun recoverStuckPhases(): Int {
    val nowNanos: Long = Timestamps.toNanos(Timestamps.fromMillis(clock.millis()))
    val thresholdNanos: Long = stalenessThreshold.toNanos()
    var recovered = 0
    for (upload in listUploads(RawImpressionUpload.State.ACTIVE)) {
      for (modelLine in listUploadModelLines(upload.name)) {
        if (nowNanos - Timestamps.toNanos(modelLine.updateTime) <= thresholdNanos) {
          continue
        }
        val recoveredThis =
          when (modelLine.state) {
            RawImpressionUploadModelLine.State.RANKING ->
              recoverIfAllRankerJobsSucceeded(upload.name, modelLine.cmmsModelLine)
            RawImpressionUploadModelLine.State.LABELING ->
              recoverIfAllVidLabelingJobsSucceeded(upload.name, modelLine.cmmsModelLine)
            // POOL_ASSIGNING recovery deferred to #4044 (PoolAssignmentJobService not in base).
            else -> false
          }
        if (recoveredThis) {
          recovered++
          VidLabelingMonitorMetrics.phaseTransitionsRecoveredCounter.add(
            1,
            dataProviderAttributes(),
          )
        }
      }
    }
    return recovered
  }

  /**
   * O(1) check (List `total_size`) that every `RankerJob` for `(uploadName, modelLine)` is
   * SUCCEEDED; if so, re-publishes one of their WorkItems to advance RANKING.
   */
  private suspend fun recoverIfAllRankerJobsSucceeded(
    uploadName: String,
    modelLine: String,
  ): Boolean {
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
      return false
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
      return false
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
        .firstOrNull() ?: return false
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
  ): Boolean {
    if (vidLabelingJobsInState(uploadName, modelLine, VidLabelingJob.State.CREATED).isNotEmpty()) {
      return false
    }
    if (vidLabelingJobsInState(uploadName, modelLine, VidLabelingJob.State.FAILED).isNotEmpty()) {
      return false
    }
    val job =
      vidLabelingJobsInState(uploadName, modelLine, VidLabelingJob.State.SUCCEEDED).firstOrNull()
        ?: return false
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
   * Re-publishes the existing WorkItem named `workItems/[workItemId]` under a deterministic
   * recovery id by fetching its queue + params and creating a fresh WorkItem (there is no
   * re-enqueue RPC, and re-creating with the same id no-ops on ALREADY_EXISTS). Returns whether a
   * new recovery WorkItem was published (a benign ALREADY_EXISTS means one is already in flight).
   */
  private suspend fun republishWorkItem(workItemId: String): Boolean {
    val existing =
      try {
        workItemsStub.getWorkItem(getWorkItemRequest { name = "workItems/$workItemId" })
      } catch (e: StatusException) {
        logger.warning("Cannot recover: WorkItem $workItemId unavailable (${e.status.code})")
        return false
      }
    return try {
      workItemsStub.createWorkItem(
        createWorkItemRequest {
          this.workItemId = "$workItemId-monitor-recovery"
          workItem = workItem {
            queue = existing.queue
            workItemParams = existing.workItemParams
          }
        }
      )
      logger.info("Recovered a stuck transition by re-publishing WorkItem $workItemId")
      true
    } catch (e: StatusException) {
      if (e.status.code == Status.Code.ALREADY_EXISTS) {
        // A recovery WorkItem is already in flight for this transition; nothing more to do.
        false
      } else {
        throw e
      }
    }
  }

  private fun dataProviderAttributes(): Attributes =
    Attributes.of(VidLabelingMonitorMetrics.DATA_PROVIDER_ATTR, dataProviderName)

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
