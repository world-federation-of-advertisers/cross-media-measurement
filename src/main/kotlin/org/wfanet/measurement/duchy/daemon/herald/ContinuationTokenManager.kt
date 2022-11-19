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

import org.wfanet.measurement.internal.duchy.ContinuationTokensGrpcKt.ContinuationTokensCoroutineStub
import org.wfanet.measurement.internal.duchy.getContinuationTokenRequest
import org.wfanet.measurement.internal.duchy.setContinuationTokenRequest

/** Manager for continuation tokens received along with Computations during streaming. */
class ContinuationTokenManager(
  private val continuationTokenClient: ContinuationTokensCoroutineStub
) {
  enum class State {
    PENDING,
    PROCESSED,
  }

  private val continuationTokens: LinkedHashMap<String, State> = LinkedHashMap()
  private var latestContinuationToken = ""

  /**
   * Get the latest continuation token to stream computations and clear the [continuationTokens] for
   * the incoming stream.
   *
   * @return the latest continuation token for the next stream.
   */
  suspend fun getLatestContinuationToken(): String {
    synchronized(this) {
      continuationTokens.clear()
      if (latestContinuationToken.isNotEmpty()) {
        return latestContinuationToken
      }
    }

    val response =
      continuationTokenClient
        .withWaitForReady()
        .getContinuationToken(getContinuationTokenRequest {})

    synchronized(this) {
      latestContinuationToken = response.token
      return latestContinuationToken
    }
  }

  /**
   * Add a continuationToken of [State.PENDING] into [continuationTokens].
   *
   * The caller is responsible to guarantee insertions are consistent with the time order of the
   * received Computations.
   */
  fun addPendingToken(continuationToken: String) {
    synchronized(this) { continuationTokens[continuationToken] = State.PENDING }
  }

  /**
   * Mark a continuation token as [State.PROCESSED]. Update the latest continuation token to
   * persistent storage when all prior tokens are all [State.PROCESSED].
   */
  suspend fun markTokenProcessed(token: String) {
    var lastProcessedToken = ""

    synchronized(this) {
      continuationTokens[token] = State.PROCESSED

      for (item in continuationTokens) {
        if (item.value == State.PENDING) {
          break
        }
        lastProcessedToken = item.key
      }
      latestContinuationToken = lastProcessedToken
    }

    if (lastProcessedToken != "") {
      // TODO(@renjiez): Throttle the calling of the api if needed.
      continuationTokenClient.setContinuationToken(
        setContinuationTokenRequest { this.token = latestContinuationToken }
      )
    }
  }
}
