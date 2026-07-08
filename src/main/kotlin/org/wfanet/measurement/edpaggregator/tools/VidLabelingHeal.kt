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
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.gcloud.pubsub.DefaultGooglePubSubClient
import org.wfanet.measurement.gcloud.pubsub.Publisher
import org.wfanet.measurement.gcloud.pubsub.Subscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
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
    [MarkFailedCommand::class, RedeliverDlqCommand::class, CommandLine.HelpCommand::class],
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

  /** Builds a mutual-TLS channel to the EDP Aggregator public API. */
  protected fun buildEdpaChannel(): ManagedChannel {
    val clientCerts =
      SigningCerts.fromPemFiles(
        certificateFile = tlsFlags.certFile,
        privateKeyFile = tlsFlags.privateKeyFile,
        trustedCertCollectionFile = tlsFlags.certCollectionFile,
      )
    return buildMutualTlsChannel(edpaPublicApiTarget, clientCerts, edpaPublicApiCertHost)
  }

  companion object {
    /** Maximum time to wait for a gRPC channel to terminate during shutdown. */
    const val SHUTDOWN_TIMEOUT_SECONDS = 30L
  }
}

/**
 * Force-marks a stuck or hanging `(upload, model line)` `FAILED`, unblocking subsequent uploads.
 *
 * Use for a hung TEE processor or an upload stale beyond its SLA — cases the Monitor only alerts
 * on. The reason is recorded as the model line's `error_message`.
 */
@Command(
  name = "mark-failed",
  description =
    ["Force-marks a stuck/hanging (upload, model line) FAILED, unblocking queued uploads."],
  mixinStandardHelpOptions = true,
)
class MarkFailedCommand : EdpaApiCommand() {
  @Option(
    names = ["--data-provider"],
    description = ["DataProvider resource name (e.g. dataProviders/{data_provider})."],
    required = true,
  )
  private lateinit var dataProvider: String

  @Option(
    names = ["--upload-id"],
    description = ["RawImpressionUpload id segment."],
    required = true,
  )
  private lateinit var uploadId: String

  @Option(
    names = ["--model-line"],
    description = ["CMMS ModelLine resource name of the model line to fail."],
    required = true,
  )
  private lateinit var modelLine: String

  @Option(
    names = ["--reason"],
    description = ["Operator diagnosis, recorded as the model line's error_message."],
    required = true,
  )
  private lateinit var reason: String

  override fun run() {
    val channel: ManagedChannel = buildEdpaChannel()
    try {
      runBlocking {
        val healer = VidLabelingHealer(RawImpressionUploadModelLineServiceCoroutineStub(channel))
        val updated = healer.markFailed(dataProvider, uploadId, modelLine, reason)
        println("Marked ${updated.name} FAILED (state=${updated.state}).")
      }
    } finally {
      channel.shutdown()
      channel.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)
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

fun main(args: Array<String>) = commandLineMain(VidLabelingHeal(), args)
