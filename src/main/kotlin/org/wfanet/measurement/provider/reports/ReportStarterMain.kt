package org.wfanet.measurement.provider.reports

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

object ReportStarterFlags {
  val MAX_PARALLELISM = intFlag("max-parallelism", 32)
}

object ThrottlerFlags {
  val OVERLOAD_FACTOR = doubleFlag("throttler-overload_factor", 1.2)
  val TIME_HORIZON = durationFlag("throttler-time-horizon", Duration.ofMinutes(2))
  val POLL_DELAY_MILLIS = longFlag("throttler-poll-delay-millis", 0)
}

fun main(args: Array<String>) {
  runBlocking {
    Flags.parse(args.asIterable())

    val throttler = AdaptiveThrottler(
      ThrottlerFlags.OVERLOAD_FACTOR.value,
      Clock.systemUTC(),
      ThrottlerFlags.TIME_HORIZON.value,
      ThrottlerFlags.POLL_DELAY_MILLIS.value
    )

    val reportStarter = ReportStarter(
      ReportApiImpl(throttler),
      ReportStarterFlags.MAX_PARALLELISM.value
    )

    // We just launch each of the tasks that we want to do in parallel. They each run indefinitely.
    launch { reportStarter.createReports() }
    launch { reportStarter.createRequisitions() }
    launch { reportStarter.startComputations() }
  }
}
