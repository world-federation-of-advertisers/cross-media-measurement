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

package org.wfanet.measurement.common

import java.time.Duration
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.withTimeoutOrNull

/**
 * Enables periodically restarting Flow generators.
 *
 * One canonical use case is to disguise a paginated API as a continuous stream.
 *
 * Another canonical use case is to restart a streaming API from time to time for better
 * load-balancing and to get fresher results.
 *
 * renewedFlow will go on forever -- so run it in a coroutine and cancel when appropriate.
 *
 * @param reconnectMillis how frequently to restart the stream
 * @param reconnectDelayMillis after disconnecting, how long to wait until reconnecting
 * @param block callback that will be called repeatedly to produce flows
 * @return the combined flow
 */
fun <T> renewedFlow(
  reconnectMillis: Long,
  reconnectDelayMillis: Long,
  block: suspend () -> Flow<T>
): Flow<T> = flow {
  while (true) {
    withTimeoutOrNull(reconnectMillis) {
      emitAll(block())
    }
    delay(reconnectDelayMillis)
  }
}

fun <T> renewedFlow(
  reconnect: Duration,
  reconnectDelay: Duration,
  block: suspend () -> Flow<T>
): Flow<T> =
  renewedFlow(reconnect.toMillis(), reconnectDelay.toMillis(), block)
