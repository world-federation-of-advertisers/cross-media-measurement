// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.deploy.common.daemon.herald

import java.io.File
import java.time.Clock
import java.time.Duration
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.common.identity.withDuchyId
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.duchy.daemon.herald.Herald
import org.wfanet.measurement.duchy.deploy.common.CommonDuchyFlags
import org.wfanet.measurement.duchy.deploy.common.ComputationsServiceFlags
import org.wfanet.measurement.duchy.deploy.common.SystemApiFlags
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.config.ProtocolsSetupConfig
import org.wfanet.measurement.system.v1alpha.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineStub as SystemComputationParticipantsCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationsGrpcKt.ComputationsCoroutineStub as SystemComputationsCoroutineStub
import picocli.CommandLine

private class Flags {
  @CommandLine.Mixin
  lateinit var duchy: CommonDuchyFlags
    private set

  @CommandLine.Mixin
  lateinit var tlsFlags: TlsFlags
    private set

  @CommandLine.Option(
    names = ["--channel-shutdown-timeout"],
    defaultValue = "3s",
    description = ["How long to allow for the gRPC channel to shutdown."],
  )
  lateinit var channelShutdownTimeout: Duration
    private set

  @CommandLine.Option(
    names = ["--polling-interval"],
    defaultValue = "1m",
    description = ["How long to sleep between calls to the system Computations Service."],
  )
  lateinit var pollingInterval: Duration
    private set

  @CommandLine.Mixin
  lateinit var systemApiFlags: SystemApiFlags
    private set

  @CommandLine.Mixin
  lateinit var computationsServiceFlags: ComputationsServiceFlags
    private set

  @CommandLine.Option(
    names = ["--protocols-setup-config"],
    description = ["ProtocolsSetupConfig proto message in text format."],
    required = true
  )
  lateinit var protocolsSetupConfig: File
    private set
}

@CommandLine.Command(
  name = "HeraldDaemon",
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

  val systemServiceChannel =
    buildMutualTlsChannel(flags.systemApiFlags.target, clientCerts, flags.systemApiFlags.certHost)
      .withShutdownTimeout(flags.channelShutdownTimeout)
  val systemComputationsClient =
    SystemComputationsCoroutineStub(systemServiceChannel).withDuchyId(flags.duchy.duchyName)
  val systemComputationParticipantsClient =
    SystemComputationParticipantsCoroutineStub(systemServiceChannel)
      .withDuchyId(flags.duchy.duchyName)

  val storageChannel =
    buildMutualTlsChannel(
        flags.computationsServiceFlags.target,
        clientCerts,
        flags.computationsServiceFlags.certHost
      )
      .withShutdownTimeout(flags.channelShutdownTimeout)

  // This will be the name of the pod when deployed to Kubernetes.
  val heraldId = System.getenv("HOSTNAME")

  val herald =
    Herald(
      heraldId = heraldId,
      duchyId = flags.duchy.duchyName,
      internalComputationsClient = ComputationsCoroutineStub(storageChannel),
      systemComputationsClient = systemComputationsClient,
      systemComputationParticipantClient = systemComputationParticipantsClient,
      protocolsSetupConfig =
        flags.protocolsSetupConfig.reader().use {
          parseTextProto(it, ProtocolsSetupConfig.getDefaultInstance())
        },
      clock = Clock.systemUTC()
    )
  val pollingThrottler = MinimumIntervalThrottler(Clock.systemUTC(), flags.pollingInterval)
  runBlocking { herald.continuallySyncStatuses(pollingThrottler) }
}

fun main(args: Array<String>) = commandLineMain(::run, args)
