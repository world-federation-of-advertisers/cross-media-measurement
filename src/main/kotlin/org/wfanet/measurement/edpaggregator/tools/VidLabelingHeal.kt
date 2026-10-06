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
      ListCorrectionCandidatesCommand::class,
      GetCorrectionCandidateCommand::class,
      ListCorrectionPlansCommand::class,
      GetCorrectionPlanCommand::class,
      ApproveCorrectionPlanCommand::class,
      RetryCorrectionPlanCommand::class,
      RedeliverDlqCommand::class,
      // TODO(world-federation-of-advertisers/cross-media-measurement#4223): add
      // HealRankIndexCommand after terminal model lines can be reopened for re-ranking.
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
    val channel = buildEdpaChannel()
    try {
      runBlocking {
        val failed =
          DispatchFailer(
              RawImpressionUploadServiceCoroutineStub(channel),
              RawImpressionUploadModelLineServiceCoroutineStub(channel),
            )
            .failUpload(rawImpressionUpload, reason)
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
          "the furthest phase reached."
      ],
  )
  private var fromPhase: RawImpressionUploadModelLine.State? = null

  override fun run() {
    val edpaChannel = buildEdpaChannel()
    val controlPlaneChannel = buildChannel(controlPlaneApiTarget, controlPlaneApiCertHost)
    try {
      runBlocking {
        val result =
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
            .retryFailed(rawImpressionUpload, modelLine, fromPhase)
        if (result.wasAlreadyStarted) {
          println(
            "Retry for ${result.modelLineName} was already started; current state is " +
              "${result.newState}."
          )
        } else {
          println(
            "Re-triggered ${result.modelLineName} at ${result.newState}: created " +
              "${result.workItemsRepublished} retry WorkItem(s)."
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
    description = ["Stop after this many milliseconds elapse with no new message."],
    defaultValue = "10000",
  )
  private var idleTimeoutMillis: Long = 10000

  @Option(
    names = ["--topic-override"],
    description = ["Republish every message to this topic instead of its recorded queue."],
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
    /** Messages pulled per Pub/Sub request; the total is bounded by `--max-messages`. */
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
    description = ["Comma-separated RawImpressionUpload resource names to backfill."],
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
    val channel = buildEdpaChannel()
    try {
      runBlocking {
        val result =
          ModelLineBackfiller(RawImpressionUploadModelLineServiceCoroutineStub(channel))
            .backfill(modelLine, rawImpressionUploads)
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

fun main(args: Array<String>) = commandLineMain(VidLabelingHeal(), args)
