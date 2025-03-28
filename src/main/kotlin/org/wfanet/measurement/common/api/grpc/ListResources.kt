/*
 * Copyright 2024 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.common.api.grpc

import com.google.protobuf.Message
import io.grpc.kotlin.AbstractCoroutineStub
import kotlin.coroutines.coroutineContext
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flattenConcat
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map

/** A [List] of resources from a paginated List method. */
data class ResourceList<R : Message, T>(
  val resources: List<R>,
  /**
   * A token that can be sent on subsequent requests to retrieve the next page. If this is an empty
   * page token, there are no subsequent pages.
   *
   * If [T] is [String], then the empty page token is `""`. If [T] is nullable, then the empty page
   * token is `null`.
   */
  val nextPageToken: T,
) : List<R> by resources

/**
 * Lists resources from a paginated List method on this stub.
 *
 * @param pageToken page token for initial request
 * @param list function which calls the appropriate List method on the stub
 */
inline fun <R : Message, reified T, S : AbstractCoroutineStub<S>> S.listResources(
  pageToken: T = getEmptyPageToken(),
  crossinline list: suspend S.(pageToken: T) -> ResourceList<R, T>,
): Flow<ResourceList<R, T>> =
  listResources(Int.MAX_VALUE, pageToken) { nextPageToken, _ -> list(nextPageToken) }

/**
 * Lists resources from a paginated List method on this stub.
 *
 * @param limit maximum number of resources to emit
 * @param pageToken page token for initial request
 * @param list function which calls the appropriate List method on the stub, returning no more than
 *   the specified remaining number of resources
 */
inline fun <R : Message, reified T, S : AbstractCoroutineStub<S>> S.listResources(
  limit: Int,
  pageToken: T = getEmptyPageToken(),
  crossinline list: suspend S.(pageToken: T, remaining: Int) -> ResourceList<R, T>,
): Flow<ResourceList<R, T>> {
  require(limit > 0) { "limit must be positive" }
  val emptyPageToken: T = getEmptyPageToken()
  return flow {
    var remaining: Int = limit
    var nextPageToken = pageToken

    while (true) {
      coroutineContext.ensureActive()

      val resourceList: ResourceList<R, T> = list(nextPageToken, remaining)
      require(resourceList.size <= remaining) {
        "List call must ensure that limit is not exceeded. " +
          "Returned ${resourceList.size} items when only $remaining were remaining"
      }
      emit(resourceList)

      remaining -= resourceList.size
      nextPageToken = resourceList.nextPageToken
      if (nextPageToken == emptyPageToken || remaining == 0) {
        break
      }
    }
  }
}

/** @see [flattenConcat] */
@ExperimentalCoroutinesApi // Overloads experimental `flattenConcat` function.
fun <R : Message, T> Flow<ResourceList<R, T>>.flattenConcat(): Flow<R> =
  map { it.asFlow() }.flattenConcat()

@PublishedApi
internal inline fun <reified T> getEmptyPageToken(): T {
  return if (T::class == String::class) {
    "" as T
  } else if (null is T) { // T is nullable.
    null as T
  } else {
    error("Unhandled page token type")
  }
}
