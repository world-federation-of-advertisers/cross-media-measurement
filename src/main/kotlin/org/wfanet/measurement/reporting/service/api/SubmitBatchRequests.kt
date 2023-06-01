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
suspend fun <ITEM, RESP, RESULT> submitBatchRequests(
  items: Flow<ITEM>,
  limit: Int,
  rpcCall: suspend (List<ITEM>) -> RESP,
  parseResponse: (RESP) -> List<RESULT>
): Flow<RESULT> {
  if (limit <= 0) {
    throw BatchRequestException(
      "Invalid limit",
      IllegalArgumentException("The size limit of a batch must be greater than 0.")
    )
  }

  return items.chunked(limit).flatMapConcat { batch -> parseResponse(rpcCall(batch)).asFlow() }
}
