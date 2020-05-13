package org.wfanet.measurement.common

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.produceIn
import kotlinx.coroutines.launch

/**
 * Spawns a number of coroutines to read from the channel in parallel.
 *
 * @param[scope] the scope for sub-coroutines.
 * @param[max_parallelism] the number of coroutines to launch to read from the channel
 * @param[block] what to do with non-throttled channel elements
 */
suspend fun <T> ReceiveChannel<T>.consumeInParallel(
  scope: CoroutineScope,
  max_parallelism: Int,
  block: suspend (T) -> Unit
) {
  repeat(max_parallelism) {
    scope.launch {
      for (item in this@consumeInParallel) {
        block(item)
      }
    }
  }
}

/**
 * Reads from the channel in parallel, subject to a throttler.
 *
 * Any exception from the block is treated as a signal to throttle. Also, all exceptions are
 * suppressed, so errors should be propagated via alternate means.
 *
 * @param[scope] the scope for sub-coroutines.
 * @param[max_parallelism] the number of coroutines to launch to read from the channel
 * @param[throttler] limits throughput
 * @param[block] what to do with non-throttled channel elements
 */
suspend fun <T> ReceiveChannel<T>.throttledConsumeEach(
  scope: CoroutineScope,
  max_parallelism: Int,
  throttler: Throttler,
  block: suspend (T) -> Unit
) {
  consumeInParallel(scope, max_parallelism) { item: T ->
    throttler.onReady {
      block(item)
    }
  }
}

/**
 * Helper to apply ReceiveChannel.consumeInParallel to a Flow.
 *
 * Since flows are sequential, this is a convenient way to do a parallel map over a flow.
 *
 * @param[max_parallelism] the number of coroutines to launch to read from the channel
 * @param[throttler] limits throughput
 * @param[block] what to do with non-throttled channel elements
 */
@OptIn(kotlinx.coroutines.FlowPreview::class)
suspend fun <T> Flow<T>.parallelCollect(
  max_parallelism: Int,
  block: suspend (T) -> Unit
) = coroutineScope {
  this@parallelCollect.produceIn(this)
    .consumeInParallel(this, max_parallelism, block)
}

/**
 * Helper to apply ReceiveChannel.throttledCollect to a Flow.
 *
 * Since flows are sequential, this is a convenient way to do a rate-limited parallel map over a
 * flow.
 *
 * @param[max_parallelism] the number of coroutines to launch to read from the channel
 * @param[throttler] limits throughput
 * @param[block] what to do with non-throttled channel elements
 */
@OptIn(kotlinx.coroutines.FlowPreview::class)
suspend fun <T> Flow<T>.throttledCollect(
  max_parallelism: Int,
  throttler: Throttler,
  block: suspend (T) -> Unit
) = coroutineScope {
  this@throttledCollect.produceIn(this)
    .throttledConsumeEach(this, max_parallelism, throttler, block)
}
