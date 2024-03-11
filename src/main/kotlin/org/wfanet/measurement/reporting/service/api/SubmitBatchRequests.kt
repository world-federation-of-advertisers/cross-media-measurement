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

import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit

class BatchRequestException(message: String? = null, cause: Throwable? = null) :
  Exception(message, cause)

/**
 * Splits this flow into a flow of lists each not exceeding the given size.
 *
 * The last list in the resulting list may have fewer elements than the given size.
 */
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

/**
 * Submits multiple RPCs by dividing the input items to batches.
 *
 * @return [Flow] that emits [List]s containing the results of the multiple RPCs.
 */
suspend fun <ITEM, RESP, RESULT> submitBatchRequests(
  items: Flow<ITEM>,
  limit: Int,
  callRpc: suspend (List<ITEM>) -> RESP,
  parseResponse: (RESP) -> List<RESULT>,
): Flow<List<RESULT>> {
  if (limit <= 0) {
    throw BatchRequestException(
      "Invalid limit",
      IllegalArgumentException("The size limit of a batch must be greater than 0."),
    )
  }

  // For network requests, the number of concurrent coroutines needs to be capped. To be on the safe
  // side, a low number is chosen.
  val batchSemaphore = Semaphore(3)
  return flow {
    coroutineScope {
      val deferred: List<Deferred<List<RESULT>>> = buildList {
        items.chunked(limit).collect { batch: List<ITEM> ->
          // The batch reference is reused for every collect call. To ensure async works, a copy
          // of the contents needs to be saved in a new reference.
          val tempBatch = batch.toList()
          add(async { batchSemaphore.withPermit { parseResponse(callRpc(tempBatch)) } })
        }
      }

      deferred.forEach { emit(it.await()) }
    }
  }
}
