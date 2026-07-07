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
import io.grpc.Status
import io.grpc.StatusException
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

/**
 * Lists resources from a paginated List method on this stub with an adaptive page size.
 *
 * The user-supplied [list] lambda receives the page token and the page size to use. On a
 * `RESOURCE_EXHAUSTED` failure from the server (typically the gRPC inbound message size limit, e.g.
 * 4MB), the page size is halved and the same page is retried until either the call succeeds or the
 * page size has been reduced to [minPageSize] (at which point the original error is rethrown). Once
 * a reduced size has worked, the smaller size is used for subsequent pages to avoid repeating the
 * doomed-large-page step on every page.
 *
 * @param startingPageSize initial page size for the first request.
 * @param minPageSize floor below which the page size will not be reduced; if a request still fails
 *   with `RESOURCE_EXHAUSTED` at this size, the error is rethrown.
 * @param pageToken page token for the initial request.
 * @param onPageSizeReduced callback invoked whenever the page size is reduced after
 *   `RESOURCE_EXHAUSTED`. Useful for emitting metrics or logging.
 * @param list function which calls the appropriate List method on the stub using the supplied page
 *   token and page size.
 */
inline fun <R : Message, reified T, S : AbstractCoroutineStub<S>> S
  .listResourcesWithAdaptivePageSize(
  startingPageSize: Int,
  minPageSize: Int = 1,
  pageToken: T = getEmptyPageToken(),
  crossinline onPageSizeReduced: (oldPageSize: Int, newPageSize: Int) -> Unit = { _, _ -> },
  crossinline list: suspend S.(pageToken: T, pageSize: Int) -> ResourceList<R, T>,
): Flow<ResourceList<R, T>> {
  require(startingPageSize >= minPageSize) {
    "startingPageSize ($startingPageSize) must be >= minPageSize ($minPageSize)"
  }
  require(minPageSize >= 1) { "minPageSize must be at least 1" }
  val emptyPageToken: T = getEmptyPageToken()
  return flow {
    var nextPageToken = pageToken
    var pageSize = startingPageSize
    while (true) {
      coroutineContext.ensureActive()

      var resourceList: ResourceList<R, T>? = null
      while (resourceList == null) {
        try {
          resourceList = list(nextPageToken, pageSize)
        } catch (e: StatusException) {
          if (e.status.code != Status.Code.RESOURCE_EXHAUSTED || pageSize <= minPageSize) {
            throw e
          }
          val reduced = (pageSize / 2).coerceAtLeast(minPageSize)
          onPageSizeReduced(pageSize, reduced)
          pageSize = reduced
        }
      }
      emit(resourceList)

      nextPageToken = resourceList.nextPageToken
      if (nextPageToken == emptyPageToken) {
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
