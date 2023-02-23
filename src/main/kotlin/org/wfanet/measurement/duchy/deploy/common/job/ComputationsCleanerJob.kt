// Copyright 2023 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.duchy.deploy.common.job

import io.grpc.Channel
import java.time.Duration
import kotlin.properties.Delegates
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withDefaultDeadline
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.duchy.deploy.common.ComputationsServiceFlags
import org.wfanet.measurement.duchy.service.internal.computations.ComputationsCleaner
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import picocli.CommandLine

private class Flags {
  @CommandLine.Mixin
  lateinit var tlsFlags: TlsFlags
    private set

  @CommandLine.Mixin
  lateinit var computationsServiceFlags: ComputationsServiceFlags
    private set

  @CommandLine.Option(
    names = ["--computations-time-to-live"],
    defaultValue = "90d",
    description =
      ["Time to live (TTL) for a Computation. Computations live longer than this will be cleaned."],
  )
  lateinit var computationsTimeToLive: Duration
    private set

  @CommandLine.Option(
    names = ["--channel-shutdown-timeout"],
    defaultValue = "3s",
    description = ["How long to allow for the gRPC channel to shutdown."],
  )
  lateinit var channelShutdownTimeout: Duration
    private set

  @set:CommandLine.Option(
    names = ["--debug-verbose-grpc-client-logging"],
    description = ["Enables full gRPC request and response logging for outgoing gRPCs"],
    defaultValue = "false"
  )
  var verboseGrpcClientLogging by Delegates.notNull<Boolean>()
    private set
}

@CommandLine.Command(
  name = "ComputationsCleanerJob",
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private fun run(@CommandLine.Mixin flags: Flags) {
  val clientCerts =
    SigningCerts.fromPemFiles(
      certificateFile = flags.tlsFlags.certFile,
      privateKeyFile = flags.tlsFlags.privateKeyFile,
      trustedCertCollectionFile = flags.tlsFlags.certCollectionFile
    )
  val internalComputationsChannel: Channel =
    buildMutualTlsChannel(
        flags.computationsServiceFlags.target,
        clientCerts,
        flags.computationsServiceFlags.certHost
      )
      .withShutdownTimeout(flags.channelShutdownTimeout)
      .withDefaultDeadline(flags.computationsServiceFlags.defaultDeadlineDuration)
      .withVerboseLogging(flags.verboseGrpcClientLogging)
  val internalComputationsClient = ComputationsCoroutineStub(internalComputationsChannel)

  val computationsCleaner =
    ComputationsCleaner(internalComputationsClient, flags.computationsTimeToLive)
  computationsCleaner.run()
}

fun main(args: Array<String>) = commandLineMain(::run, args)
