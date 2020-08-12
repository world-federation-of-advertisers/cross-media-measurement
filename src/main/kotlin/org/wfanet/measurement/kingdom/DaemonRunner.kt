package org.wfanet.measurement.kingdom

import io.grpc.ManagedChannel
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
import picocli.CommandLine

class DaemonFlags {
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

fun runDaemon(flags: DaemonFlags, block: suspend Daemon.() -> Unit) = runBlocking {
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

  Daemon(throttler, flags.maxParallelism, reportStarterClient).block()
}
