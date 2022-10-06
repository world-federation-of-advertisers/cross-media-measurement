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

/** Stores the latest continuation token. Support read and write operation */
interface ContinuationTokenStore {
  suspend fun readContinuationToken(): String

  suspend fun updateContinuationToken(continuationToken: String)
}

class ContinuationTokenManager(
    val duchyName: String,
    private val continuationTokenStore: ContinuationTokenStore
) {
  private data class TokenEntry(val token: String, var state: State) {
    enum class State {
      UNPROCESSED,
      PROCESSED,
    }
  }

  // Items are in Pairs of (continuationToken: String, processed: Boolean).
  private val continuationTokenList: MutableList<TokenEntry> =
      Collections.synchronizedList(mutableListOf())

  // Note: Also clear the continuationTokenList for an incoming streaming.
  suspend fun getLatestContinuationToken(): String {
    continuationTokenList.clear()
    return continuationTokenStore.readContinuationToken()
  }

  fun addContinuationToken(continuationToken: String): Int {
    val index = continuationTokenList.size
    continuationTokenList += TokenEntry(continuationToken, TokenEntry.State.UNPROCESSED)
    return index
  }

  suspend fun updateContinuationToken(index: Int) {
    require(index < continuationTokenList.size)

    continuationTokenList[index].state = TokenEntry.State.PROCESSED
    val firstUnprocessedIndex =
        continuationTokenList.indexOfFirst { it.state == TokenEntry.State.UNPROCESSED }
    val lastProcessedIndex =
        if (firstUnprocessedIndex == -1) {
          continuationTokenList.lastIndex
        } else {
          firstUnprocessedIndex - 1
        }
    if (lastProcessedIndex >= 0) {
      val lastProcessedToken = continuationTokenList[lastProcessedIndex].token
      continuationTokenStore.updateContinuationToken(lastProcessedToken)
    }
  }
}
