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
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.edpaggregator.VidLabelingRpcDurationConverter
import org.wfanet.measurement.edpaggregator.VidLabelingRpcThrottlers
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
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
      // TODO(world-federation-of-advertisers/cross-media-measurement#4223): add
      // HealRankIndexCommand
      // to rebuild a corrupted cumulative rank-index SNAPSHOT from retained inputs. A COMPLETED
      // model
      // line is terminal today, so this needs a reopen-for-re-rank transition or a ranker TEE heal
      // mode — see the issue.
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
 * Restarts from the furthest phase the model line actually reached, auto-detected from which
 * per-phase job rows exist: `VidLabelingJob`s ⇒ Phase 2 (`LABELING`), else `RankerJob`s ⇒ Phase 1
 * (`RANKING`), else `PoolAssignmentJob`s ⇒ Phase 0 (`POOL_ASSIGNING`). The operator can override
 * the detected phase with `--from-phase`. It re-publishes that phase's WorkItem(s) and transitions
 * the model line out of `FAILED`; the pipeline's idempotency gates skip already-succeeded work, so
 * it resumes at the actual failure point.
 *
 * Talks to both the EDP Aggregator public API (model line + job rows) and the Secure Computation
 * control plane (WorkItems), so it takes a second target for the control plane.
 */
@Command(
  name = "retry-failed",
  description =
    [
      "Re-triggers a FAILED (upload, model line) from the furthest phase it reached " +
        "(auto-detected; override with --from-phase)."
    ],
  mixinStandardHelpOptions = true,
)
class RetryFailedCommand : EdpaApiCommand() {
  @Option(
    names = ["--metadata-read-rpc-min-interval"],
    description = ["Minimum interval between outbound metadata read RPCs."],
    defaultValue = "100ms",
    converter = [VidLabelingRpcDurationConverter::class],
  )
  private lateinit var metadataReadRpcMinInterval: Duration

  @Option(
    names = ["--metadata-write-rpc-min-interval"],
    description = ["Minimum interval between outbound metadata write RPCs."],
    defaultValue = "200ms",
    converter = [VidLabelingRpcDurationConverter::class],
  )
  private lateinit var metadataWriteRpcMinInterval: Duration

  @Option(
    names = ["--control-plane-rpc-min-interval"],
    description = ["Minimum interval between outbound Secure Computation control-plane RPCs."],
    defaultValue = "250ms",
    converter = [VidLabelingRpcDurationConverter::class],
  )
  private lateinit var controlPlaneRpcMinInterval: Duration

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

  @Option(
    names = ["--from-phase"],
    description =
      [
        "Override the phase to re-trigger from (POOL_ASSIGNING, RANKING, or LABELING). Default: " +
          "the furthest phase the model line reached, auto-detected from existing job rows."
      ],
    required = false,
  )
  private var fromPhase: RawImpressionUploadModelLine.State? = null

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
            RankerJobServiceCoroutineStub(edpaChannel),
            VidLabelingJobServiceCoroutineStub(edpaChannel),
            WorkItemsCoroutineStub(controlPlaneChannel),
            VidLabelingRpcThrottlers.fromMinimumIntervals(
              metadataRead = metadataReadRpcMinInterval,
              metadataWrite = metadataWriteRpcMinInterval,
              controlPlane = controlPlaneRpcMinInterval,
            ),
          )
        val result = retrier.retryFailed(rawImpressionUpload, modelLine, fromPhase)
        if (result.workItemsRepublished == 0) {
          System.err.println(
            "WARN: all target WorkItems for ${result.modelLineName} already exist from a prior " +
              "retry; the model line remains ${result.newState} and no new work was created. " +
              "Investigate the prior retry's WorkItems (workItems/rt-<...>) before re-running."
          )
        } else {
          println(
            "Re-triggered ${result.modelLineName} at ${result.newState}: republished " +
              "${result.workItemsRepublished} WorkItem(s)."
          )
        }
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
    require(ModelLineKey.fromName(modelLine) != null) {
      "--model-line must be a valid CMMS ModelLine resource name " +
        "(modelProviders/.../modelSuites/.../modelLines/...); got '$modelLine'"
    }
    require(rawImpressionUploads.all { it.isNotBlank() }) {
      "--raw-impression-uploads entries must be non-blank RawImpressionUpload resource names."
    }
    val channel: ManagedChannel = buildEdpaChannel()
    try {
      runBlocking {
        val backfiller =
          ModelLineBackfiller(RawImpressionUploadModelLineServiceCoroutineStub(channel))
        val result = backfiller.backfill(modelLine, rawImpressionUploads)
        println(
          "Backfilled $modelLine: created ${result.createdModelLines.size} model line(s) " +
            "(creating a model line reactivates its COMPLETED parent upload)."
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
 * retention window. Prints the cascade and prompts the operator to confirm before mutating
 * anything.
 *
 * Confirmation is interactive (type `yes`), by design, not a `--confirm` flag: the printed cascade
 * often includes uploads the operator did not name explicitly (later uploads that cascade from the
 * earliest bad one), so making the operator visually review that cascade before typing `yes` is the
 * whole safety property — a `--confirm` flag baked into a runbook or shell history would bypass it.
 * It also fails closed: a non-interactive invocation (no TTY / EOF) reads null and declines, so
 * cron/CI cannot silently mutate (see [isAffirmative]).
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

  override fun run() {
    require(ModelLineKey.fromName(modelLine) != null) {
      "--model-line must be a valid CMMS ModelLine resource name " +
        "(modelProviders/.../modelSuites/.../modelLines/...); got '$modelLine'"
    }
    require(badUploads.all { it.isNotBlank() }) {
      "--bad-uploads entries must be non-blank RawImpressionUpload resource names."
    }
    require(retentionDays > 0) { "--retention-days must be positive; got $retentionDays" }
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

        // Require explicit operator confirmation before any mutation.
        print(
          "This marks the ${plan.cascade.size} model line(s) above FAILED and soft-deletes their " +
            "cumulative snapshots. Type 'yes' to proceed: "
        )
        System.out.flush()
        if (!isAffirmative(readLine())) {
          println("Aborted; nothing was changed.")
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

/**
 * Returns true iff [answer] is an affirmative confirmation — "y" or "yes" (case-insensitive,
 * surrounding whitespace trimmed). A null answer (no stdin / EOF) is treated as a decline, so the
 * `evict-uploads` prompt fails closed when the command is run without an interactive terminal.
 */
fun isAffirmative(answer: String?): Boolean {
  val normalized: String? = answer?.trim()?.lowercase()
  return normalized == "y" || normalized == "yes"
}

fun main(args: Array<String>) = commandLineMain(VidLabelingHeal(), args)
