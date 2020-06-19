package org.wfanet.measurement.kingdom

import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import java.time.Clock
import java.time.Duration
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.AdaptiveThrottler
import org.wfanet.measurement.common.Flags
import org.wfanet.measurement.common.doubleFlag
import org.wfanet.measurement.common.durationFlag
import org.wfanet.measurement.common.intFlag
import org.wfanet.measurement.common.longFlag
import org.wfanet.measurement.common.stringFlag
import org.wfanet.measurement.internal.kingdom.ReportConfigStorageGrpcKt.ReportConfigStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReportStorageGrpcKt.ReportStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.RequisitionStorageGrpcKt.RequisitionStorageCoroutineStub

object ReportStarterFlags {
  val MAX_PARALLELISM = intFlag("max-parallelism", 32)
  val INTERNAL_SERVICES_TARGET = stringFlag("internal-services-target", "")
}

object ThrottlerFlags {
  val OVERLOAD_FACTOR = doubleFlag("throttler-overload-factor", 1.2)
  val TIME_HORIZON = durationFlag("throttler-time-horizon", Duration.ofMinutes(2))
  val POLL_DELAY_MILLIS = longFlag("throttler-poll-delay-millis", 0)
}

fun main(args: Array<String>) {
  runBlocking {
    Flags.parse(args.asIterable())

    val channel: ManagedChannel =
      ManagedChannelBuilder
        .forTarget(ReportStarterFlags.INTERNAL_SERVICES_TARGET.value)
        .build()

    val throttler = AdaptiveThrottler(
      ThrottlerFlags.OVERLOAD_FACTOR.value,
      Clock.systemUTC(),
      ThrottlerFlags.TIME_HORIZON.value,
      ThrottlerFlags.POLL_DELAY_MILLIS.value
    )

    val reportStarterClient = ReportStarterClientImpl(
      ReportConfigStorageCoroutineStub(channel),
      ReportStorageCoroutineStub(channel),
      RequisitionStorageCoroutineStub(channel)
    )

    val reportStarter = ReportStarter(
      throttler,
      ReportStarterFlags.MAX_PARALLELISM.value,
      reportStarterClient
    )

    // We just launch each of the tasks that we want to do in parallel. They each run indefinitely.
    // TODO: move to separate binaries.
    launch { reportStarter.createReports() }
    launch { reportStarter.createRequisitions() }
    launch { reportStarter.startReports() }
  }
}
