// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.daemon.herald

import java.util.Collections

interface ContinuationTokenStore {
  suspend fun readContinuationToken(): String

  suspend fun writeContinuationToken(continuationToken: String): Unit
}

class ContinuationTokenManager(
  val duchyName: String,
  private val continuationTokenStore: ContinuationTokenStore
) {
  // Items are in Pairs of (continuationToken: String, processed: Boolean).
  private val continuationTokenList: MutableList<Pair<String, Boolean>> =
    Collections.synchronizedList(mutableListOf())

  suspend fun getContinuationToken(): String {
    return continuationTokenStore.readContinuationToken()
  }

  suspend fun syncContinuationToken(continueToken: String, block: suspend () -> Unit) {
    val seqNumber = continuationTokenList.size

    continuationTokenList += Pair(continueToken, false)
    block()
    continuationTokenList[seqNumber] = Pair(continueToken, true)

    val firstUnprocessedIndex = continuationTokenList.indexOfFirst { !it.second }
    val lastProcessedIndex =
      if (firstUnprocessedIndex == -1) {
        continuationTokenList.lastIndex
      } else {
        firstUnprocessedIndex - 1
      }
    if (lastProcessedIndex >= 0) {
      val lastProcessedToken = continuationTokenList[lastProcessedIndex].first
      continuationTokenStore.writeContinuationToken(lastProcessedToken)
    }
  }
}
