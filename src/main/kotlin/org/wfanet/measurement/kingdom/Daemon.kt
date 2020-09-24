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

import java.time.Duration
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
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
 * @property[maxConcurrency] the maximum number of simultaneous RPCs to use per public API method
 * @property[daemonDatabaseServicesClient] a wrapper around stubs for internal services
 */
class Daemon(
  val throttler: Throttler,
  val maxConcurrency: Int,
  val daemonDatabaseServicesClient: DaemonDatabaseServicesClient
) : CoroutineScope {
  val logger: Logger = Daemon.logger

  override val coroutineContext: CoroutineContext = Dispatchers.IO

  fun <T> retryLoop(block: suspend () -> Flow<T>): Flow<T> =
    renewedFlow(Duration.ofMinutes(10), Duration.ofSeconds(1)) {
      throttleAndLog(block) ?: emptyFlow()
    }

  suspend fun <T> throttleAndLog(block: suspend () -> T): T? =
    logAndSuppressExceptionSuspend { throttler.onReadyGrpc(block) }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
