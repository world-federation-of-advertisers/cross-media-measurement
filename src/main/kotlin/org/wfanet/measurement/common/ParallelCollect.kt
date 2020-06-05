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
 * @param[maxParallelism] the number of coroutines to launch to read from the channel
 * @param[block] what to do with non-throttled channel elements
 */
suspend fun <T> ReceiveChannel<T>.consumeInParallel(
  scope: CoroutineScope,
  maxParallelism: Int,
  block: suspend (T) -> Unit
) {
  repeat(maxParallelism) {
    scope.launch {
      for (item in this@consumeInParallel) {
        block(item)
      }
    }
  }
}

/**
 * Helper to apply ReceiveChannel.consumeInParallel to a Flow.
 *
 * Since flows are sequential, this is a convenient way to do a parallel map over a flow.
 *
 * @param[maxParallelism] the number of coroutines to launch to read from the channel
 * @param[block] what to do with non-throttled channel elements
 */
@OptIn(kotlinx.coroutines.FlowPreview::class)
suspend fun <T> Flow<T>.parallelCollect(
  maxParallelism: Int,
  block: suspend (T) -> Unit
) = coroutineScope {
  this@parallelCollect.produceIn(this)
    .consumeInParallel(this, maxParallelism, block)
}
