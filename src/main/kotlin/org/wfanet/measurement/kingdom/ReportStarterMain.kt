// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.kingdom

import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import java.time.Clock
import java.time.Duration
import kotlin.properties.Delegates
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.AdaptiveThrottler
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.internal.kingdom.ReportConfigScheduleStorageGrpcKt.ReportConfigScheduleStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReportConfigStorageGrpcKt.ReportConfigStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReportStorageGrpcKt.ReportStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.RequisitionStorageGrpcKt.RequisitionStorageCoroutineStub
import picocli.CommandLine

private class Flags {
  @set:CommandLine.Option(
    names = ["--max-parallelism"],
    defaultValue = "32"
  )
  var maxParallelism by Delegates.notNull<Int>()
    private set

  @CommandLine.Option(
    names = ["--internal-services-target"],
    required = true
  )
  lateinit var internalServicesTarget: String
    private set

  @set:CommandLine.Option(
    names = ["--throttler-overload-factor"],
    defaultValue = "1.2"
  )
  var overloadFactor by Delegates.notNull<Double>()
    private set

  @CommandLine.Option(
    names = ["--throttler-time-horizon"],
    defaultValue = "2m"
  )
  lateinit var timeHorizon: Duration
    private set

  @CommandLine.Option(
    names = ["--throttler-poll-delay"],
    defaultValue = "1ms"
  )
  lateinit var pollDelay: Duration
    private set
}

@CommandLine.Command(
  name = "report_starter_main",
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private fun run(@CommandLine.Mixin flags: Flags) = runBlocking {
  val channel: ManagedChannel =
    ManagedChannelBuilder
      .forTarget(flags.internalServicesTarget)
      .usePlaintext()
      .build()

  val throttler = AdaptiveThrottler(
    flags.overloadFactor,
    Clock.systemUTC(),
    flags.timeHorizon,
    flags.pollDelay
  )

  val reportStarterClient = ReportStarterClientImpl(
    ReportConfigStorageCoroutineStub(channel),
    ReportConfigScheduleStorageCoroutineStub(channel),
    ReportStorageCoroutineStub(channel),
    RequisitionStorageCoroutineStub(channel)
  )

  val reportStarter = ReportStarter(
    throttler,
    flags.maxParallelism,
    reportStarterClient
  )

  // We just launch each of the tasks that we want to do in parallel. They each run indefinitely.
  // TODO: move to separate binaries.
  launch { reportStarter.createReports() }
  launch { reportStarter.createRequisitions() }
  launch { reportStarter.startReports() }
}

fun main(args: Array<String>) = commandLineMain(::run, args)
