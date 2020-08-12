package org.wfanet.measurement.kingdom

import java.time.Duration
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import org.wfanet.measurement.common.Throttler
import org.wfanet.measurement.common.logAndSuppressExceptionSuspend
import org.wfanet.measurement.common.onReadyGrpc
import org.wfanet.measurement.common.renewedFlow

/**
 * Class representing daemons in the Kingdom. Daemons themselves are implemented as extension
 * functions on Daemon for easy access to the properties.
 *
 * @property[throttler] a throttler to rate-limit gRPCs
 * @property[maxParallelism] the maximum number of sub-coroutines to use per public API method
 * @property[daemonDatabaseServicesClient] a wrapper around stubs for internal services
 */
class Daemon(
  val throttler: Throttler,
  val maxParallelism: Int,
  val daemonDatabaseServicesClient: DaemonDatabaseServicesClient
) {
  fun <T> retryLoop(block: suspend () -> Flow<T>): Flow<T> =
    renewedFlow(Duration.ofMinutes(10), Duration.ZERO) {
      throttleAndLog(block) ?: emptyFlow()
    }

  suspend fun <T> throttleAndLog(block: suspend () -> T): T? =
    logAndSuppressExceptionSuspend { throttler.onReadyGrpc(block) }
}
