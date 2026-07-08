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

package org.wfanet.measurement.edpaggregator.tools

import io.grpc.ManagedChannel
import java.time.Duration
import java.time.Instant
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub
import org.wfanet.measurement.gcloud.pubsub.DefaultGooglePubSubClient
import org.wfanet.measurement.gcloud.pubsub.Publisher
import org.wfanet.measurement.gcloud.pubsub.Subscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import picocli.CommandLine
import picocli.CommandLine.Command
import picocli.CommandLine.Mixin
import picocli.CommandLine.Option

/**
 * Operator tool to recover the VID labeling pipeline from failure states.
 *
 * Each sub-command targets a failure mode that requires operator judgment; automated recovery
 * (stuck-phase advancement, dispatch sequencing) is handled by the `VidLabelingMonitorFunction`.
 * Connection flags live on the individual sub-commands, since not every command talks to the same
 * backend (e.g. `redeliver-dlq` uses Pub/Sub, not the EDP Aggregator public API).
 */
@Command(
  name = "vid-labeling-heal",
  description = ["Operator tool to recover the VID labeling pipeline from failure states."],
  mixinStandardHelpOptions = true,
  subcommands =
    [
      MarkFailedCommand::class,
      RetryFailedCommand::class,
      BackfillModelLineCommand::class,
      EvictUploadsCommand::class,
      RedeliverDlqCommand::class,
      CommandLine.HelpCommand::class,
    ],
)
class VidLabelingHeal : Runnable {
  /** Prints usage when invoked without a sub-command. */
  override fun run() {
    CommandLine(this).usage(System.err)
  }
}

/** Base for sub-commands that call the EDP Aggregator public API over mutual TLS. */
abstract class EdpaApiCommand : Runnable {
  @Mixin protected lateinit var tlsFlags: TlsFlags

  @Option(
    names = ["--edpa-public-api-target"],
    description = ["gRPC target (host:port) of the EDP Aggregator public API."],
    required = true,
  )
  protected lateinit var edpaPublicApiTarget: String

  @Option(
    names = ["--edpa-public-api-cert-host"],
    description =
      [
        "Expected hostname in the EDP Aggregator public API TLS certificate, if it differs from " +
          "the target host."
      ],
    required = false,
  )
  protected var edpaPublicApiCertHost: String? = null

  /** Builds a mutual-TLS channel to [target] using the shared client certs. */
  protected fun buildChannel(target: String, certHost: String?): ManagedChannel {
    val clientCerts =
      SigningCerts.fromPemFiles(
        certificateFile = tlsFlags.certFile,
        privateKeyFile = tlsFlags.privateKeyFile,
        trustedCertCollectionFile = tlsFlags.certCollectionFile,
      )
    return buildMutualTlsChannel(target, clientCerts, certHost)
  }

  /** Builds a mutual-TLS channel to the EDP Aggregator public API. */
  protected fun buildEdpaChannel(): ManagedChannel =
    buildChannel(edpaPublicApiTarget, edpaPublicApiCertHost)

  companion object {
    /** Maximum time to wait for a gRPC channel to terminate during shutdown. */
    const val SHUTDOWN_TIMEOUT_SECONDS = 30L
  }
}

/**
 * Force-fails a stuck or hanging `RawImpressionUpload`, unblocking subsequent uploads.
 *
 * Use for a hung TEE processor or an upload stale beyond its SLA — cases the Monitor only alerts
 * on. Marks every non-terminal child model line `FAILED` (leaving COMPLETED / already-FAILED ones
 * untouched); the parent upload transitions to FAILED via the child cascade. The reason is recorded
 * as each failed model line's `error_message`.
 */
@Command(
  name = "mark-failed",
  description =
    ["Force-fails a stuck/hanging upload's in-progress model lines, unblocking queued uploads."],
  mixinStandardHelpOptions = true,
)
class MarkFailedCommand : EdpaApiCommand() {
  @Option(
    names = ["--raw-impression-upload"],
    description =
      ["RawImpressionUpload resource name (dataProviders/{dp}/rawImpressionUploads/{upload})."],
    required = true,
  )
  private lateinit var rawImpressionUpload: String

  @Option(
    names = ["--reason"],
    description = ["Operator diagnosis, recorded as each failed model line's error_message."],
    required = true,
  )
  private lateinit var reason: String

  override fun run() {
    val channel: ManagedChannel = buildEdpaChannel()
    try {
      runBlocking {
        val failer = DispatchFailer(RawImpressionUploadModelLineServiceCoroutineStub(channel))
        val failed = failer.failUpload(rawImpressionUpload, reason)
        println("Marked ${failed.size} model line(s) FAILED under $rawImpressionUpload.")
      }
    } finally {
      channel.shutdown()
      channel.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)
    }
  }
}

/**
 * Re-triggers a `FAILED` `(upload, model line)` after the operator has resolved the root cause.
 *
 * Restarts from the beginning of the model line's path — Phase 0 (`POOL_ASSIGNING`) for a memoized
 * model line, Phase 2 (`LABELING`) for a non-memoized one (detected from whether Phase-0 jobs
 * exist) — by re-publishing that phase's WorkItem(s) and transitioning the model line out of
 * `FAILED`. The pipeline's idempotency gates skip already-succeeded work, so it resumes at the
 * actual failure point.
 *
 * Talks to both the EDP Aggregator public API (model line + job rows) and the Secure Computation
 * control plane (WorkItems), so it takes a second target for the control plane.
 */
@Command(
  name = "retry-failed",
  description = ["Re-triggers a FAILED (upload, model line) from the start of its path."],
  mixinStandardHelpOptions = true,
)
class RetryFailedCommand : EdpaApiCommand() {
  @Option(
    names = ["--control-plane-api-target"],
    description = ["gRPC target (host:port) of the Secure Computation control-plane API."],
    required = true,
  )
  private lateinit var controlPlaneApiTarget: String

  @Option(
    names = ["--control-plane-api-cert-host"],
    description = ["Expected hostname in the control-plane API TLS certificate, if it differs."],
    required = false,
  )
  private var controlPlaneApiCertHost: String? = null

  @Option(
    names = ["--raw-impression-upload"],
    description =
      ["RawImpressionUpload resource name (dataProviders/{dp}/rawImpressionUploads/{upload})."],
    required = true,
  )
  private lateinit var rawImpressionUpload: String

  @Option(
    names = ["--model-line"],
    description = ["CMMS ModelLine resource name of the failed model line."],
    required = true,
  )
  private lateinit var modelLine: String

  override fun run() {
    val edpaChannel: ManagedChannel = buildEdpaChannel()
    val controlPlaneChannel: ManagedChannel =
      buildChannel(controlPlaneApiTarget, controlPlaneApiCertHost)
    try {
      runBlocking {
        val retrier =
          FailedDispatchRetrier(
            RawImpressionUploadModelLineServiceCoroutineStub(edpaChannel),
            PoolAssignmentJobServiceCoroutineStub(edpaChannel),
            VidLabelingJobServiceCoroutineStub(edpaChannel),
            WorkItemsCoroutineStub(controlPlaneChannel),
          )
        val result = retrier.retryFailed(rawImpressionUpload, modelLine)
        val startPhase = if (result.memoized) "Phase 0 (POOL_ASSIGNING)" else "Phase 2 (LABELING)"
        println(
          "Re-triggered ${result.modelLineName} from $startPhase: republished " +
            "${result.workItemsRepublished} WorkItem(s), state=${result.newState}."
        )
      }
    } finally {
      edpaChannel.shutdown()
      controlPlaneChannel.shutdown()
      edpaChannel.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)
      controlPlaneChannel.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)
    }
  }
}

/**
 * Redelivers dead-lettered `WorkItem`s from a Pub/Sub dead-letter subscription back onto their
 * origin work queues, resuming processing after the operator has fixed the underlying issue.
 */
@Command(
  name = "redeliver-dlq",
  description =
    ["Redelivers dead-lettered WorkItems from a dead-letter subscription to their origin queues."],
  mixinStandardHelpOptions = true,
)
class RedeliverDlqCommand : Runnable {
  @Option(
    names = ["--dlq-subscription"],
    description = ["Pub/Sub subscription id of the dead-letter queue (e.g. <queue>-dlq-sub)."],
    required = true,
  )
  private lateinit var dlqSubscription: String

  @Option(
    names = ["--google-project-id"],
    description = ["Google Cloud project id hosting the Pub/Sub topics/subscriptions."],
    required = true,
  )
  private lateinit var googleProjectId: String

  @Option(
    names = ["--max-messages"],
    description = ["Maximum number of messages to redeliver in this run."],
    defaultValue = "1000",
  )
  private var maxMessages: Int = 1000

  @Option(
    names = ["--idle-timeout-millis"],
    description = ["Stop after this many milliseconds elapse with no new message (queue drained)."],
    defaultValue = "10000",
  )
  private var idleTimeoutMillis: Long = 10000

  @Option(
    names = ["--topic-override"],
    description =
      [
        "Republish every message to this topic instead of the origin queue recorded on the " +
          "WorkItem. Only needed when the WorkItem does not carry its queue."
      ],
    required = false,
  )
  private var topicOverride: String? = null

  override fun run() {
    val pubSubClient = DefaultGooglePubSubClient()
    val subscriber = Subscriber(googleProjectId, pubSubClient, maxMessages = PULL_BATCH_SIZE)
    val publisher = Publisher<WorkItem>(googleProjectId, pubSubClient)
    try {
      val redelivered = runBlocking {
        DlqRedeliverer(subscriber, publisher)
          .redeliver(dlqSubscription, maxMessages, idleTimeoutMillis, topicOverride)
      }
      println("Redelivered $redelivered message(s) from $dlqSubscription.")
    } finally {
      subscriber.close()
      publisher.close()
    }
  }

  companion object {
    /** Messages pulled per Pub/Sub request; the total is bounded by --max-messages. */
    private const val PULL_BATCH_SIZE = 10
  }
}

/**
 * Backfills a new model line onto existing (COMPLETED) uploads so it is labeled over historical
 * data without a data-provider re-upload (Backfill Path B). Creates a `CREATED`
 * `RawImpressionUploadModelLine` for the model line under each upload, then reactivates the parent
 * upload so the Monitor dispatches it.
 */
@Command(
  name = "backfill-model-line",
  description = ["Adds a model line to existing COMPLETED uploads and reactivates them."],
  mixinStandardHelpOptions = true,
)
class BackfillModelLineCommand : EdpaApiCommand() {
  @Option(
    names = ["--model-line"],
    description = ["CMMS ModelLine resource name to backfill onto the uploads."],
    required = true,
  )
  private lateinit var modelLine: String

  @Option(
    names = ["--raw-impression-uploads"],
    description =
      [
        "Comma-separated RawImpressionUpload resource names " +
          "(dataProviders/{dp}/rawImpressionUploads/{upload}) to backfill the model line onto."
      ],
    required = true,
    split = ",",
  )
  private lateinit var rawImpressionUploads: List<String>

  override fun run() {
    val channel: ManagedChannel = buildEdpaChannel()
    try {
      runBlocking {
        val backfiller =
          ModelLineBackfiller(
            RawImpressionUploadModelLineServiceCoroutineStub(channel),
            RawImpressionUploadServiceCoroutineStub(channel),
          )
        val result = backfiller.backfill(modelLine, rawImpressionUploads)
        println(
          "Backfilled $modelLine: created ${result.createdModelLines.size} model line(s), " +
            "reactivated ${result.reactivatedUploads.size} upload(s)."
        )
      }
    } finally {
      channel.shutdown()
      channel.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)
    }
  }
}

/**
 * Evicts uploads that carry bad data for a model line. Marks the bad upload and every later upload
 * for the model line FAILED and soft-deletes their cumulative SNAPSHOT rank-index blobs, so Phase-1
 * falls back to the last good snapshot when the affected uploads are re-triggered. Confined to the
 * retention window. Prints the cascade and only mutates when `--confirm` is passed.
 */
@Command(
  name = "evict-uploads",
  description =
    ["Evicts a bad upload and all later uploads for a model line (rebuild-from-last-good)."],
  mixinStandardHelpOptions = true,
)
class EvictUploadsCommand : EdpaApiCommand() {
  @Option(
    names = ["--model-line"],
    description = ["CMMS ModelLine resource name whose uploads are being evicted."],
    required = true,
  )
  private lateinit var modelLine: String

  @Option(
    names = ["--bad-uploads"],
    description =
      [
        "Comma-separated bad RawImpressionUpload resource names " +
          "(dataProviders/{dp}/rawImpressionUploads/{upload}); the earliest anchors the cascade."
      ],
    required = true,
    split = ",",
  )
  private lateinit var badUploads: List<String>

  @Option(
    names = ["--retention-days"],
    description = ["Retention window in days; uploads created before now-retention are rejected."],
    required = true,
  )
  private var retentionDays: Int = 0

  @Option(
    names = ["--reason"],
    description = ["Operator diagnosis, recorded as each evicted model line's error_message."],
    required = true,
  )
  private lateinit var reason: String

  @Option(
    names = ["--confirm"],
    description = ["Actually perform the eviction. Without it the command only prints the cascade."],
  )
  private var confirm: Boolean = false

  override fun run() {
    val channel: ManagedChannel = buildEdpaChannel()
    try {
      runBlocking {
        val evictUploader =
          EvictUploader(
            RawImpressionUploadServiceCoroutineStub(channel),
            RawImpressionUploadModelLineServiceCoroutineStub(channel),
            RankIndexBlobServiceCoroutineStub(channel),
          )
        val cutoffTime: Instant = Instant.now().minus(Duration.ofDays(retentionDays.toLong()))
        val plan = evictUploader.plan(modelLine, badUploads, cutoffTime)

        println(
          "Eviction cascade for $modelLine (${plan.cascade.size} upload(s)): " +
            plan.cascade.map { it.uploadName }
        )
        if (plan.extraUploads.isNotEmpty()) {
          println(
            "NOTE: uploads created after the bad one(s) will also be evicted: ${plan.extraUploads}"
          )
        }
        if (!confirm) {
          println(
            "Dry run. Re-run with --confirm to mark these model lines FAILED and soft-delete their " +
              "cumulative snapshots."
          )
          return@runBlocking
        }

        val result = evictUploader.evict(modelLine, plan, reason)
        println(
          "Evicted: marked ${result.failedModelLines.size} model line(s) FAILED, soft-deleted " +
            "${result.deletedSnapshots} snapshot(s). Re-trigger the affected uploads (re-upload " +
            "their done blobs) to rebuild from the last good snapshot."
        )
      }
    } finally {
      channel.shutdown()
      channel.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)
    }
  }
}

fun main(args: Array<String>) = commandLineMain(VidLabelingHeal(), args)
