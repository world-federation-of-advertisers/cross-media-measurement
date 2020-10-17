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

package org.wfanet.measurement.kingdom.deploy.common.daemon

import io.grpc.Channel
import java.time.Clock
import java.time.Duration
import kotlin.properties.Delegates
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.grpc.buildChannel
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.common.throttler.AdaptiveThrottler
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedulesGrpcKt.ReportConfigSchedulesCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReportConfigsGrpcKt.ReportConfigsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.kingdom.daemon.Daemon
import org.wfanet.measurement.kingdom.daemon.DaemonDatabaseServicesClientImpl
import picocli.CommandLine

class DaemonFlags {
  @set:CommandLine.Option(
    names = ["--max-concurrency"],
    defaultValue = "32"
  )
  var maxConcurrency by Delegates.notNull<Int>()
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

  @CommandLine.Option(
    names = ["--retry-poll-delay"],
    defaultValue = "10s"
  )
  lateinit var retryPollDelay: Duration
    private set

  @set:CommandLine.Option(
    names = ["--debug-verbose-grpc-client-logging"],
    description = ["Enables full gRPC request and response logging for outgoing gRPCs"],
    defaultValue = "false"
  )
  var debugVerboseGrpcClientLogging by Delegates.notNull<Boolean>()
    private set
}

fun runDaemon(flags: DaemonFlags, block: suspend Daemon.() -> Unit) = runBlocking {
  val channel: Channel =
    buildChannel(flags.internalServicesTarget)
      .withVerboseLogging(flags.debugVerboseGrpcClientLogging)

  val throttler = AdaptiveThrottler(
    flags.overloadFactor,
    Clock.systemUTC(),
    flags.timeHorizon,
    flags.pollDelay
  )

  val databaseClient = DaemonDatabaseServicesClientImpl(
    ReportConfigsCoroutineStub(channel),
    ReportConfigSchedulesCoroutineStub(channel),
    ReportsCoroutineStub(channel),
    RequisitionsCoroutineStub(channel)
  )

  Daemon(throttler, flags.maxConcurrency, databaseClient, flags.retryPollDelay).block()
}
