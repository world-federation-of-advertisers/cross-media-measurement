/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.integration.k8s

import io.grpc.ManagedChannel
import io.grpc.Status
import io.grpc.StatusException
import java.nio.file.Path
import java.nio.file.Paths
import java.util.logging.Logger
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJob
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJob
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJob
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.listPoolAssignmentJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRankerJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listVidLabelingJobsRequest

/**
 * JUnit [TestRule] that drives + asserts the VID Labeling pipeline for the pipelined EDP (edp7):
 * after [SeedRawImpressionsRule] drops the raw `done` marker (which the DataWatcher turns into a
 * `VidLabelingDispatcher` fast-path start), this rule polls the EDP-Aggregator public API until the
 * whole state machine reaches its terminal, and fails the run if any part FAILs or it times out.
 *
 * ## What it asserts
 * The single authoritative terminal signal is `RawImpressionUploadModelLine.State.COMPLETED`: the
 * "last job out" only flips a (upload, model line) to `COMPLETED` after its Phase-0
 * `PoolAssignmentJob`, Phase-1 `RankerJob`, and Phase-2 `VidLabelingJob` have all `SUCCEEDED`. So
 * this rule waits for **every** model line of **every** edp7 `RawImpressionUpload` to reach
 * `COMPLETED` (failing fast on any `FAILED`), then best-effort verifies each phase's job reached
 * `SUCCEEDED` (logged; not fatal if the job listing is unavailable).
 *
 * Runs after the pipeline has been triggered and before the measurement rules, so the measurement
 * reads edp7's freshly-labeled output. Authentication is edp7 mutual-TLS against the EDPA public
 * API (trust root `edp_aggregator_root.pem`).
 *
 * Env-provided inputs; the rule is a **no-op** unless both are set:
 * * `EDPA_PUBLIC_API_TARGET` — the EDP-Aggregator public API
 *   (`system.edp-aggregator.<env>...:8443`).
 * * `PIPELINED_DATA_PROVIDER` — the EDP whose uploads to poll (edp7).
 */
class AwaitVidLabelingRule : TestRule {

  override fun apply(base: Statement, description: Description): Statement {
    return object : Statement() {
      override fun evaluate() {
        if (EDPA_PUBLIC_API_TARGET.isEmpty() || PIPELINED_DATA_PROVIDER.isEmpty()) {
          logger.warning(
            "EDPA_PUBLIC_API_TARGET/PIPELINED_DATA_PROVIDER unset; skipping VID Labeling await."
          )
        } else {
          runBlocking { awaitCompletion() }
        }
        base.evaluate()
      }
    }
  }

  private suspend fun awaitCompletion() {
    val channel: ManagedChannel = buildEdpaChannel()
    try {
      val uploadsStub = RawImpressionUploadServiceCoroutineStub(channel)
      val modelLinesStub = RawImpressionUploadModelLineServiceCoroutineStub(channel)

      // Snapshot uploads that already exist (from earlier or failed runs) BEFORE waiting. The
      // dispatcher creates THIS run's upload only after [SeedRawImpressionsRule] drops the raw
      // `done` blob (which ran just before this rule), so any upload already present now is stale.
      // We then wait exclusively for a NEW upload — a leftover COMPLETED upload from a prior run
      // must not short-circuit the wait (which would let the test proceed before this run's
      // pipeline actually runs). Identity (resource name) is used rather than a create-time
      // comparison to avoid CI-runner vs. Spanner commit-clock skew.
      val preexistingUploadNames: Set<String> =
        checkNotNull(
          withTimeoutOrNull(TIMEOUT_MS) {
            while (true) {
              try {
                return@withTimeoutOrNull listUploads(uploadsStub).map { it.name }.toSet()
              } catch (e: StatusException) {
                if (e.status.code !in RETRYABLE_CODES) throw e
                logger.warning(
                  "transient ${e.status.code} snapshotting uploads (retrying): ${e.message}"
                )
                delay(POLL_MS)
              }
            }
            @Suppress("UNREACHABLE_CODE") null
          }
        ) {
          "Timed out snapshotting pre-existing edp7 uploads before awaiting VID Labeling."
        }
      logger.info(
        "Ignoring ${preexistingUploadNames.size} pre-existing edp7 upload(s) from earlier runs; " +
          "awaiting only this run's upload."
      )

      var lastSummary = "(no fresh uploads yet)"
      val terminal =
        withTimeoutOrNull(TIMEOUT_MS) {
          while (true) {
            try {
              val uploads: List<RawImpressionUpload> =
                listUploads(uploadsStub).filter { it.name !in preexistingUploadNames }
              if (uploads.isEmpty()) {
                lastSummary = "no new RawImpressionUpload for $PIPELINED_DATA_PROVIDER yet"
              } else {
                val lines: List<RawImpressionUploadModelLine> =
                  uploads.flatMap { listModelLines(modelLinesStub, it.name) }
                val failed = lines.filter { it.state == RawImpressionUploadModelLine.State.FAILED }
                check(failed.isEmpty()) {
                  "VID Labeling FAILED for ${failed.size} model line(s): " +
                    failed.joinToString { it.name }
                }
                lastSummary = summarize(uploads, lines)
                if (lines.isNotEmpty() && lines.all { it.state == COMPLETED }) {
                  return@withTimeoutOrNull true
                }
              }
            } catch (e: StatusException) {
              // The metadata API can briefly return UNAVAILABLE/INTERNAL while its pods roll (e.g.
              // just after a terraform redeploy) or on a transient network blip. Retry within the
              // overall timeout rather than failing the whole run; a genuinely FAILED model line
              // still fails fast via check() above (IllegalStateException, not caught here).
              if (e.status.code !in RETRYABLE_CODES) throw e
              lastSummary = "transient ${e.status.code} from metadata API (retrying): ${e.message}"
              logger.warning(lastSummary)
            }
            logger.info("Waiting for VID Labeling to complete… $lastSummary")
            delay(POLL_MS)
          }
          @Suppress("UNREACHABLE_CODE") false
        }
      checkNotNull(terminal) {
        "Timed out after ${TIMEOUT_MS / 1000}s waiting for edp7 VID Labeling. Last state: $lastSummary"
      }
      logger.info("VID Labeling COMPLETED for this run's edp7 model lines. $lastSummary")

      try {
        verifyPhaseJobs(
          channel,
          listUploads(uploadsStub)
            .filter { it.name !in preexistingUploadNames }
            .flatMap { listModelLines(modelLinesStub, it.name) },
        )
      } catch (e: StatusException) {
        logger.warning(
          "Skipping best-effort phase-job verification (${e.status.code}): ${e.message}"
        )
      }
    } finally {
      channel.shutdown()
    }
  }

  /**
   * Best-effort per-phase check: each COMPLETED model line should have a SUCCEEDED job per phase.
   */
  private suspend fun verifyPhaseJobs(
    channel: ManagedChannel,
    modelLines: List<RawImpressionUploadModelLine>,
  ) {
    val poolStub = PoolAssignmentJobServiceCoroutineStub(channel)
    val rankerStub = RankerJobServiceCoroutineStub(channel)
    val vidStub = VidLabelingJobServiceCoroutineStub(channel)
    for (line in modelLines) {
      try {
        val pool =
          poolStub
            .listPoolAssignmentJobs(listPoolAssignmentJobsRequest { parent = line.name })
            .poolAssignmentJobsList
        val ranker =
          rankerStub.listRankerJobs(listRankerJobsRequest { parent = line.name }).rankerJobsList
        val vid =
          vidStub
            .listVidLabelingJobs(listVidLabelingJobsRequest { parent = line.name })
            .vidLabelingJobsList
        val poolOk = pool.any { it.state == PoolAssignmentJob.State.SUCCEEDED }
        val rankerOk = ranker.any { it.state == RankerJob.State.SUCCEEDED }
        val vidOk = vid.any { it.state == VidLabelingJob.State.SUCCEEDED }
        logger.info(
          "Phase jobs for ${line.name}: poolAssignment=$poolOk ranker=$rankerOk vidLabeling=$vidOk"
        )
      } catch (e: Exception) {
        logger.warning("Could not list phase jobs under ${line.name} (best-effort): ${e.message}")
      }
    }
  }

  private suspend fun listUploads(
    stub: RawImpressionUploadServiceCoroutineStub
  ): List<RawImpressionUpload> {
    val result = mutableListOf<RawImpressionUpload>()
    var pageToken = ""
    do {
      val response =
        stub.listRawImpressionUploads(
          listRawImpressionUploadsRequest {
            parent = PIPELINED_DATA_PROVIDER
            pageSize = LIST_PAGE_SIZE
            this.pageToken = pageToken
          }
        )
      result.addAll(response.rawImpressionUploadsList)
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())
    // Keep only the latest upload per done-blob path. Each rerun re-seeds the same `done` paths
    // with a new GCS generation, creating a new RawImpressionUpload; older ones (from prior or
    // failed runs) linger in non-terminal states and would otherwise block completion forever.
    // The newest upload per path is this run's.
    return result
      .groupBy { it.doneBlobUri }
      .values
      .mapNotNull { ups ->
        ups.maxByOrNull { it.createTime.seconds * 1_000_000_000L + it.createTime.nanos }
      }
  }

  private suspend fun listModelLines(
    stub: RawImpressionUploadModelLineServiceCoroutineStub,
    uploadName: String,
  ): List<RawImpressionUploadModelLine> {
    val result = mutableListOf<RawImpressionUploadModelLine>()
    var pageToken = ""
    do {
      val response =
        stub.listRawImpressionUploadModelLines(
          listRawImpressionUploadModelLinesRequest {
            parent = uploadName
            pageSize = LIST_PAGE_SIZE
            this.pageToken = pageToken
          }
        )
      result.addAll(response.rawImpressionUploadModelLinesList)
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())
    return result
  }

  private fun summarize(
    uploads: List<RawImpressionUpload>,
    lines: List<RawImpressionUploadModelLine>,
  ): String {
    val lineStates = lines.groupingBy { it.state.name }.eachCount()
    return "${uploads.size} upload(s), ${lines.size} model line(s) by state=$lineStates"
  }

  private fun buildEdpaChannel(): ManagedChannel {
    val secretFiles: Path =
      checkNotNull(getRuntimePath(WORKSPACE_PATH.resolve(SECRET_FILES_PATH))) {
        "secretfiles runtime path not found"
      }
    val signingCerts =
      SigningCerts.fromPemFiles(
        secretFiles.resolve(EDP_CERT_FILE).toFile(),
        secretFiles.resolve(EDP_KEY_FILE).toFile(),
        secretFiles.resolve(EDPA_TRUSTED_CERTS_FILE).toFile(),
      )
    return buildMutualTlsChannel(EDPA_PUBLIC_API_TARGET, signingCerts, EDPA_PUBLIC_API_CERT_HOST)
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private val COMPLETED = RawImpressionUploadModelLine.State.COMPLETED

    private val WORKSPACE_PATH: Path = Paths.get("wfa_measurement_system")
    private val SECRET_FILES_PATH: Path = Paths.get("src", "main", "k8s", "testing", "secretfiles")
    // The EDPA metadata API accepts control-plane client CAs (EdpAggregator / SecureComputation),
    // not the EDP's own Edp7 CA, so authenticate as the data-availability control-plane identity.
    private const val EDP_CERT_FILE = "data_availability_tls.pem"
    private const val EDP_KEY_FILE = "data_availability_tls.key"
    private const val EDPA_TRUSTED_CERTS_FILE = "edp_aggregator_root.pem"

    private const val LIST_PAGE_SIZE = 100
    // edp7 pipelines a single upload (2021-03-21) at roughly a Confidential-Space-cold ~15-20 min.
    // 45 min leaves ample margin while staying under the workflow's 60 min `--test_timeout`.
    private const val TIMEOUT_MS = 45L * 60L * 1000L
    private const val POLL_MS = 20L * 1000L

    // Transient gRPC codes from the metadata API (pod rollout after a deploy, network blips) that
    // should be retried within TIMEOUT_MS rather than failing the run.
    private val RETRYABLE_CODES =
      setOf(
        Status.Code.UNAVAILABLE,
        Status.Code.INTERNAL,
        Status.Code.DEADLINE_EXCEEDED,
        Status.Code.UNKNOWN,
        Status.Code.ABORTED,
      )

    private fun env(name: String): String = System.getenv(name).orEmpty()

    private val EDPA_PUBLIC_API_TARGET: String = env("EDPA_PUBLIC_API_TARGET")
    // The deployed EDPA public API server cert carries "localhost" in its SAN (like the Kingdom
    // cert) and the k8s test reaches it via an L4 endpoint, so override the authority to
    // "localhost" for SNI + hostname verification (matches kingdom_public_api_cert_host=localhost).
    private val EDPA_PUBLIC_API_CERT_HOST: String =
      System.getenv("EDPA_PUBLIC_API_CERT_HOST")?.takeIf { it.isNotEmpty() } ?: "localhost"
    private val PIPELINED_DATA_PROVIDER: String = env("PIPELINED_DATA_PROVIDER")
  }
}
