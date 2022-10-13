/*
 * Copyright 2022 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.common.api

import java.util.concurrent.ConcurrentHashMap
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope

/** A [PrincipalLookup] that memoizes all results returned by [delegate]. */
class MemoizingPrincipalLookup<T : Principal, K>(private val delegate: PrincipalLookup<T, K>) :
  PrincipalLookup<T, K> {

  private val cache = ConcurrentHashMap<K, Deferred<T?>>()

  override suspend fun getPrincipal(lookupKey: K): T? {
    return coroutineScope {
        cache.getOrPut(lookupKey) { async { delegate.getPrincipal(lookupKey) } }
      }
      .await()
  }
}

fun <T : Principal, K> PrincipalLookup<T, K>.memoizing(): PrincipalLookup<T, K> {
  if (this is MemoizingPrincipalLookup) {
    return this
  }
  return MemoizingPrincipalLookup(this)
}
