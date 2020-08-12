package org.wfanet.measurement.common

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.transform

/**
 * Pairs each item in a flow with all items produced by [block].
 *
 * For example, flowOf(1, 2, 3).pairAll { flowOf("a-$it", "b-$it") } would yield a flow with items:
 *   Pair(1, "a-1"), Pair(1, "b-1"), Pair(2, "a-2"), Pair(2, "b-2"), Pair(3, "a-3"), Pair(3, "b-3")
 */
fun <T, R> Flow<T>.pairAll(block: suspend (T) -> Flow<R>): Flow<Pair<T, R>> =
  transform { t -> emitAll(block(t).map { r -> Pair(t, r) }) }
