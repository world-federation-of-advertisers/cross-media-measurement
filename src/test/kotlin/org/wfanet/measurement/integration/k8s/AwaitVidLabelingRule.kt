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
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.runBlocking
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
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJob
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.listPoolAssignmentJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRankerJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listVidLabelingJobsRequest
import org.wfanet.measurement.integration.testing.VidLabelingAwaiter

/** Waits for the seeded uploads to complete before publishing measurement fixtures. */
class AwaitVidLabelingRule(private val expectedDoneBlobs: () -> Map<String, Long>) : TestRule {

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
      val modelLines =
        VidLabelingAwaiter(
            uploadsStub = RawImpressionUploadServiceCoroutineStub(channel),
            modelLinesStub = RawImpressionUploadModelLineServiceCoroutineStub(channel),
            pollInterval = 20.seconds,
            timeout = SETUP_TIMEOUT,
            rpcTimeout = RPC_TIMEOUT,
            maxConsecutiveReadFailures = 5,
          )
          .awaitCompletion(PIPELINED_DATA_PROVIDER, expectedDoneBlobs())
      logger.info("VID labeling COMPLETED for this run's ${modelLines.size} model line(s).")
      verifyPhaseJobs(channel, modelLines)
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
            .withDeadlineAfter(RPC_TIMEOUT.inWholeSeconds, TimeUnit.SECONDS)
            .listPoolAssignmentJobs(listPoolAssignmentJobsRequest { parent = line.name })
            .poolAssignmentJobsList
        val ranker =
          rankerStub
            .withDeadlineAfter(RPC_TIMEOUT.inWholeSeconds, TimeUnit.SECONDS)
            .listRankerJobs(listRankerJobsRequest { parent = line.name })
            .rankerJobsList
        val vid =
          vidStub
            .withDeadlineAfter(RPC_TIMEOUT.inWholeSeconds, TimeUnit.SECONDS)
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

  companion object {
    internal fun buildEdpaChannel(): ManagedChannel {
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

    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private val WORKSPACE_PATH: Path = Paths.get("wfa_measurement_system")
    private val SECRET_FILES_PATH: Path = Paths.get("src", "main", "k8s", "testing", "secretfiles")
    // The EDPA metadata API accepts control-plane client CAs (EdpAggregator / SecureComputation),
    // not the EDP's own Edp7 CA, so authenticate as the data-availability control-plane identity.
    private const val EDP_CERT_FILE = "data_availability_tls.pem"
    private const val EDP_KEY_FILE = "data_availability_tls.key"
    private const val EDPA_TRUSTED_CERTS_FILE = "edp_aggregator_root.pem"

    // Leave 30 minutes of the workflow budget for fixture publication and measurements.
    private val SETUP_TIMEOUT = 60.minutes
    private val RPC_TIMEOUT = 30.seconds

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
