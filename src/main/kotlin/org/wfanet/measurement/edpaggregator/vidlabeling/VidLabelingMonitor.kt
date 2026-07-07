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
import io.opentelemetry.api.common.Attributes
import java.time.Clock
import java.time.Duration
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.flattenConcat
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsRequest

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
  ) {
    val hasIssues: Boolean
      get() = dispatchError || stuckUploads.isNotEmpty() || failedModelLines.isNotEmpty()
  }

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
    return MonitorResult(
      dispatchedUpload = dispatch.dispatchedUpload,
      queuedUploads = dispatch.queuedUploads,
      stuckUploads = stuckUploads,
      failedModelLines = failedModelLines,
      dispatchError = false,
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

  private fun dataProviderAttributes(): Attributes =
    Attributes.of(VidLabelingMonitorMetrics.DATA_PROVIDER_ATTR, dataProviderName)

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
