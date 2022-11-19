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
import org.wfanet.measurement.internal.duchy.ContinuationTokensGrpcKt.ContinuationTokensCoroutineStub
import org.wfanet.measurement.internal.duchy.getContinuationTokenRequest
import org.wfanet.measurement.internal.duchy.setContinuationTokenRequest

/** Manager for continuation tokens received along with computations during streaming. */
class ContinuationTokenManager(
  private val continuationTokenClient: ContinuationTokensCoroutineStub
) {
  private data class TokenEntry(val token: String, var state: State) {
    enum class State {
      UNPROCESSED,
      PROCESSED,
    }
  }

  private val continuationTokenList: MutableList<TokenEntry> =
    Collections.synchronizedList(mutableListOf())

  private var latestContinuationToken: String? = null

  /**
   * Get the latest continuation token to stream computations. Note: Also clear the
   * continuationTokenList for the incoming stream.
   */
  suspend fun getLatestContinuationToken(): String {
    continuationTokenList.clear()

    latestContinuationToken =
      latestContinuationToken
        ?: continuationTokenClient
          .withWaitForReady()
          .getContinuationToken(getContinuationTokenRequest {})
          .token
    return latestContinuationToken!!
  }

  /** Add an UNPROCESSED continuation token entry into the list. */
  fun addContinuationToken(continuationToken: String): Int {
    latestContinuationToken = continuationToken
    val index = continuationTokenList.size
    continuationTokenList += TokenEntry(continuationToken, TokenEntry.State.UNPROCESSED)
    return index
  }

  /** Set the latest continuation token when a computation task finished by the herald. */
  suspend fun setContinuationToken(index: Int) {
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
      // Set the token
      val lastProcessedToken = continuationTokenList[lastProcessedIndex].token
      continuationTokenClient.setContinuationToken(
        setContinuationTokenRequest { token = lastProcessedToken }
      )
    }
  }
}
