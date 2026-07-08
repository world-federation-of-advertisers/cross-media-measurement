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
import picocli.CommandLine
import picocli.CommandLine.Command
import picocli.CommandLine.Mixin
import picocli.CommandLine.Option
import picocli.CommandLine.ParentCommand

/**
 * Operator tool to recover the VID labeling pipeline from failure states.
 *
 * Each sub-command targets a failure mode that requires operator judgment; automated recovery
 * (stuck-phase advancement, dispatch sequencing) is handled by the `VidLabelingMonitorFunction`.
 * Connection flags are shared across sub-commands via the parent command.
 */
@Command(
  name = "vid-labeling-heal",
  description = ["Operator tool to recover the VID labeling pipeline from failure states."],
  mixinStandardHelpOptions = true,
  subcommands = [MarkFailedCommand::class, CommandLine.HelpCommand::class],
)
class VidLabelingHeal : Runnable {
  @Mixin lateinit var tlsFlags: TlsFlags

  @Option(
    names = ["--edpa-public-api-target"],
    description = ["gRPC target (host:port) of the EDP Aggregator public API."],
    required = true,
  )
  lateinit var edpaPublicApiTarget: String

  @Option(
    names = ["--edpa-public-api-cert-host"],
    description =
      [
        "Expected hostname in the EDP Aggregator public API TLS certificate, if it differs from " +
          "the target host."
      ],
    required = false,
  )
  var edpaPublicApiCertHost: String? = null

  /** Builds a mutual-TLS channel to the EDP Aggregator public API. */
  fun buildChannel(): ManagedChannel {
    val clientCerts =
      SigningCerts.fromPemFiles(
        certificateFile = tlsFlags.certFile,
        privateKeyFile = tlsFlags.privateKeyFile,
        trustedCertCollectionFile = tlsFlags.certCollectionFile,
      )
    return buildMutualTlsChannel(edpaPublicApiTarget, clientCerts, edpaPublicApiCertHost)
  }

  /** Prints usage when invoked without a sub-command. */
  override fun run() {
    CommandLine(this).usage(System.err)
  }

  companion object {
    /** Maximum time to wait for the gRPC channel to terminate during shutdown. */
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
class MarkFailedCommand : Runnable {
  @ParentCommand private lateinit var parent: VidLabelingHeal

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
    val channel: ManagedChannel = parent.buildChannel()
    try {
      runBlocking {
        val healer = VidLabelingHealer(RawImpressionUploadModelLineServiceCoroutineStub(channel))
        val updated = healer.markFailed(dataProvider, uploadId, modelLine, reason)
        println("Marked ${updated.name} FAILED (state=${updated.state}).")
      }
    } finally {
      channel.shutdown()
      channel.awaitTermination(VidLabelingHeal.SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)
    }
  }
}

fun main(args: Array<String>) = commandLineMain(VidLabelingHeal(), args)
