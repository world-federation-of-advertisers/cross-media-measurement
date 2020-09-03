package org.wfanet.measurement.kingdom

import io.grpc.Channel
import io.grpc.ManagedChannelBuilder
import java.time.Clock
import java.time.Duration
import kotlin.properties.Delegates
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.AdaptiveThrottler
import org.wfanet.measurement.internal.kingdom.ReportConfigScheduleStorageGrpcKt.ReportConfigScheduleStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReportConfigStorageGrpcKt.ReportConfigStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReportStorageGrpcKt.ReportStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.RequisitionStorageGrpcKt.RequisitionStorageCoroutineStub
import org.wfanet.measurement.service.common.withVerboseLogging
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
    ManagedChannelBuilder
      .forTarget(flags.internalServicesTarget)
      .usePlaintext()
      .build()
      .withVerboseLogging(flags.debugVerboseGrpcClientLogging)

  val throttler = AdaptiveThrottler(
    flags.overloadFactor,
    Clock.systemUTC(),
    flags.timeHorizon,
    flags.pollDelay
  )

  val databaseClient = DaemonDatabaseServicesClientImpl(
    ReportConfigStorageCoroutineStub(channel),
    ReportConfigScheduleStorageCoroutineStub(channel),
    ReportStorageCoroutineStub(channel),
    RequisitionStorageCoroutineStub(channel)
  )

  Daemon(throttler, flags.maxConcurrency, databaseClient).block()
}
