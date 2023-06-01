/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.api

import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.flow
import org.wfanet.measurement.reporting.v2alpha.BatchCreateMetricsRequest

class BatchRequestException(message: String? = null, cause: Throwable? = null) :
  Exception(message, cause)

fun <T> Flow<T>.chunked(chunkSize: Int): Flow<List<T>> {
  val source = this
  val chunk = mutableListOf<T>()
  return flow {
    source.collect {
      chunk.add(it)
      if (chunk.size == chunkSize) {
        emit(chunk)
        chunk.clear()
      }
    }
    if (chunk.isNotEmpty()) {
      emit(chunk)
    }
  }
}

@OptIn(FlowPreview::class) // For `flatMapConcat`.
suspend inline fun <T, reified R> submitBatchRequests(
  items: Flow<T>,
  // limit: Int,
  crossinline rpcCall: suspend (List<T>) -> R,
  crossinline parseResponse: (R) -> List<T>
): Flow<T> {
  // Design 1. Store all batch limit here instead of many files
  val limit: Int =
    when (R::class) {
      BatchCreateMetricsRequest::class -> 1000
      else -> {
        throw BatchRequestException(
          "Unrecognized response type.",
          IllegalArgumentException("The response from the rpc call is not recognized.")
        )
      }
    }

  // Design 2. Let the caller specify the limit
  // if (limit <= 0) {
  //   throw BatchRequestException(
  //     "Invalid limit",
  //     IllegalArgumentException("The size limit of a batch must be greater than 0.")
  //   )
  // }

  // Design 3. Only keep Flow<T>.chunked and every batch request call just uses this line
  return items.chunked(limit).flatMapConcat { batch -> parseResponse(rpcCall(batch)).asFlow() }
}
