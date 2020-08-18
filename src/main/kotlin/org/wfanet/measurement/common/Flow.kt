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

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.transform

/**
 * Pairs each item in a flow with all items produced by [block].
 *
 * For example, flowOf(1, 2, 3).pairAll { flowOf("a-$it", "b-$it") } would yield a flow with items:
 *   Pair(1, "a-1"), Pair(1, "b-1"), Pair(2, "a-2"), Pair(2, "b-2"), Pair(3, "a-3"), Pair(3, "b-3")
 */
fun <T, R> Flow<T>.pairAll(block: suspend (T) -> Flow<R>): Flow<Pair<T, R>> =
  transform { t -> emitAll(block(t).map { r -> Pair(t, r) }) }

/**
 * Executes [onEachBlock] for each item in the flow. When the block throws a [Throwable] it will be
 * retried if the throwable matches the [retryPredicate].
 *
 * Unlike the retry coroutine function, [withRetriesOnEach] will only retry the failing item, and
 * will not restart the flow.
 *
 * When an item fails [maxAttempts] number of times or if the failure does not match the
 * [retryPredicate], the exception is thrown to be handled by downstream collection of the flow.
 *
 * For example,
 *
 * flowOf(1,2,3).withRetriesOnEach(5, retryPredicate) {
 *   executeFlakyOperation(it)
 * }
 *
 * Will try to run the executeFlakyOperation on each item up to five times as long as the errors
 * thrown match the [retryPredicate].
 *
 * @param maxAttempts maximum number of times to try executing [onEachBlock] of code
 * @param retryPredicate retry a failed attempt of [onEachBlock] if the throwable matches this
 *    predicate
 * @param onEachBlock block of code to execute for each item in the flow.
 */
fun <T> Flow<T>.withRetriesOnEach(
  maxAttempts: Int,
  retryPredicate: (Throwable) -> Boolean = { true },
  onEachBlock: suspend (T) -> Unit
): Flow<T> = onEach {
  repeat(maxAttempts) { idx ->
    val attempt = idx + 1
    try {
      return@onEach onEachBlock(it)
    } catch (e: Throwable) {
      if (attempt == maxAttempts || !retryPredicate(e)) throw e
    }
  }
}
