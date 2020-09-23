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

import kotlin.coroutines.coroutineContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedReceiveChannelException
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.produceIn
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

/**
 * Runs [transform] asynchronously, buffers at most [concurrency] [Deferred<R>]s, then awaits the
 * results sequentially.
 *
 * This has FIFO semantics: the order of results in the output flow is the same as the order of
 * the input. However, since [transform]'s results are run async, there is no guarantee that one will
 * complete before the next. (However, an item [concurrency] + 2 positions ahead of another is
 * guaranteed to finish first.)
 *
 * See https://github.com/Kotlin/kotlinx.coroutines/issues/1147.
 *
 * [scope] must contain the same [CoroutineContext] that the flow is collected in.
 *
 * @param scope the scope under which to launch [async] coroutines
 * @param concurrency number of Deferred that can be awaiting at once
 * @param transform the mapping function
 * @return the output of mapping [transform] over the receiver
 */
fun <T, R> Flow<T>.mapConcurrently(
  scope: CoroutineScope,
  concurrency: Int,
  transform: suspend (T) -> R
): Flow<R> {
  return map { scope.async { transform(it) } }.buffer(concurrency).map { it.await() }
}

/** An item consumed from a [Flow] with a [Flow] of remaining items. */
abstract class ConsumedFlowItem<T> : AutoCloseable {
  /** [Flow] item. */
  abstract val item: T

  /** [Flow] producing the remaining items. */
  abstract val remaining: Flow<T>

  /** Whether the [Flow] has remaining items. */
  abstract val hasRemaining: Boolean
}

/**
 * A [ConsumedFlowItem] wrapping a single item value, as if it was consumed from
 * a single-item [Flow].
 */
private class SingleConsumedFlowItem<T>(singleItem: T) : ConsumedFlowItem<T>() {
  override val item = singleItem
  override val remaining = flowOf<T>()
  override val hasRemaining = false

  override fun close() {
    // No-op.
  }
}

/**
 * Consumes the first item of the [Flow], producing a [Flow] for the remaining
 * items.
 *
 * Note that this starts a new coroutine in a separate [CoroutineScope] to
 * produce the returned [Flow] items using a [Channel]. As a result, the
 * returned [Flow] is hot.
 *
 * @return a [ConsumedFlowItem] containing the first item and the [Flow] of
 *     remaining items, or `null` if there is no first item. The caller must
 *     ensure that the returned object is [closed][ConsumedFlowItem.close].
 */
@OptIn(
  FlowPreview::class, // For `produceIn`
  ExperimentalCoroutinesApi::class // For `Channel.isClosedForReceive`.
)
suspend fun <T> Flow<T>.consumeFirst(): ConsumedFlowItem<T>? {
  val producerScope = CoroutineScope(coroutineContext)
  val channel: ReceiveChannel<T> = buffer(Channel.RENDEZVOUS).produceIn(producerScope)

  // We can't know whether the flow is empty until we start collecting. Since
  // we're using a rendezvous channel, this means the channel won't be closed
  // until after we attempt to receive the first item.
  val item = try {
    channel.receive()
  } catch (e: ClosedReceiveChannelException) {
    return null
  }

  val hasRemaining = !channel.isClosedForReceive
  return object : ConsumedFlowItem<T>() {
    override val item = item
    override val remaining = if (hasRemaining) channel.consumeAsFlow() else flowOf()
    override val hasRemaining = hasRemaining

    override fun close() {
      channel.cancel()
    }
  }
}

/** @see consumeFirst */
suspend fun <T> Flow<T>.consumeFirstOr(lazySingleItem: () -> T): ConsumedFlowItem<T> {
  return consumeFirst() ?: SingleConsumedFlowItem(lazySingleItem())
}
