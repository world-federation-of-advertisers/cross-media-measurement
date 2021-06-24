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

import java.time.Clock
import java.time.Duration
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.grpc.addChannelShutdownHooks
import org.wfanet.measurement.common.grpc.buildChannel
import org.wfanet.measurement.common.identity.DuchyInfo
import org.wfanet.measurement.common.identity.DuchyInfoFlags
import org.wfanet.measurement.common.identity.withDuchyId
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.config.PublicApiProtocolConfigs
import org.wfanet.measurement.duchy.daemon.herald.Herald
import org.wfanet.measurement.duchy.deploy.common.CommonDuchyFlags
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.config.ProtocolsSetupConfig
import org.wfanet.measurement.system.v1alpha.ComputationsGrpcKt.ComputationsCoroutineStub as SystemComputationsCoroutineStub
import picocli.CommandLine

private class Flags {
  @CommandLine.Mixin
  lateinit var duchy: CommonDuchyFlags
    private set

  @CommandLine.Mixin
  lateinit var duchyInfoFlags: DuchyInfoFlags
    private set

  @CommandLine.Option(
    names = ["--channel-shutdown-timeout"],
    defaultValue = "3s",
    description = ["How long to allow for the gRPC channel to shutdown."],
    required = true
  )
  lateinit var channelShutdownTimeout: Duration
    private set

  @CommandLine.Option(
    names = ["--polling-interval"],
    defaultValue = "1m",
    description = ["How long to sleep between calls to the Global Computation Service."],
    required = true
  )
  lateinit var pollingInterval: Duration
    private set

  @CommandLine.Option(
    names = ["--system-computation-service-target"],
    description = ["Address and port of the kingdom system Computations Service"],
    required = true
  )
  lateinit var systemComputationsServiceTarget: String
    private set

  @CommandLine.Option(
    names = ["--computations-service-target"],
    description = ["Address and port of the Computations service"],
    required = true
  )
  lateinit var computationsServiceTarget: String
    private set

  @CommandLine.Option(
    names = ["--protocols-setup-config"],
    description = ["ProtocolsSetupConfig proto message in text format."],
    required = true
  )
  lateinit var protocolsSetupConfig: String
    private set

  @CommandLine.Option(
    names = ["--public-api-protocol-configs"],
    description = ["PublicApiProtocolConfigs proto message in text format."],
    required = true
  )
  lateinit var publicApiProtocolConfigs: String
    private set
}

@CommandLine.Command(
  name = "HeraldDaemon",
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private fun run(@CommandLine.Mixin flags: Flags) {
  DuchyInfo.initializeFromFlags(flags.duchyInfoFlags)

  val duchyName = flags.duchy.duchyName
  val otherDuchyNames = DuchyInfo.ALL_DUCHY_IDS.minus(duchyName).toList()

  val systemComputationServiceChannel = buildChannel(flags.systemComputationsServiceTarget)
  val systemComputationsClient =
    SystemComputationsCoroutineStub(systemComputationServiceChannel)
      .withDuchyId(flags.duchy.duchyName)

  val storageChannel = buildChannel(flags.computationsServiceTarget)

  addChannelShutdownHooks(
    Runtime.getRuntime(),
    flags.channelShutdownTimeout,
    systemComputationServiceChannel,
    storageChannel
  )

  val herald =
    Herald(
      otherDuchiesInComputation = otherDuchyNames,
      internalComputationsClient = ComputationsCoroutineStub(storageChannel),
      systemComputationsClient = systemComputationsClient,
      protocolsSetupConfig =
        flags.protocolsSetupConfig.reader().use {
          parseTextProto(it, ProtocolsSetupConfig.getDefaultInstance())
        },
      configMaps =
        flags
          .publicApiProtocolConfigs
          .reader()
          .use { parseTextProto(it, PublicApiProtocolConfigs.getDefaultInstance()) }
          .configsMap
    )
  val pollingThrottler = MinimumIntervalThrottler(Clock.systemUTC(), flags.pollingInterval)
  runBlocking { herald.continuallySyncStatuses(pollingThrottler) }
}

fun main(args: Array<String>) = commandLineMain(::run, args)
