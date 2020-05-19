package org.wfanet.measurement.common

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
 * @param[reconnectMillis] how frequently to restart the stream
 * @param[reconnectDelayMillis] after disconnecting, how long to wait until reconnecting
 * @param[producer] callback that will be called repeatedly to produce flows
 * @return the combined flow
 */
@OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
fun <T> renewedFlow(
  reconnectMillis: Long,
  reconnectDelayMillis: Long,
  producer: suspend () -> Flow<T>
): Flow<T> = flow {
  while (true) {
    withTimeoutOrNull(reconnectMillis) {
      emitAll(producer())
    }
    delay(reconnectDelayMillis)
  }
}
