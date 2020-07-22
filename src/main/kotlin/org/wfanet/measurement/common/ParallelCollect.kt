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
