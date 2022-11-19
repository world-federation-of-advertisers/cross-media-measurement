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

import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import org.wfanet.measurement.internal.duchy.ContinuationTokensGrpcKt.ContinuationTokensCoroutineStub
import org.wfanet.measurement.internal.duchy.getContinuationTokenRequest
import org.wfanet.measurement.internal.duchy.setContinuationTokenRequest

/** Manager for continuation tokens received along with computations during streaming. */
class ContinuationTokenManager(
  private val continuationTokenClient: ContinuationTokensCoroutineStub
) {
  enum class State {
    PENDING,
    PROCESSED,
  }

  // Lock to synchronize continuationTokens and latestContinuationToken
  private val lock = ReentrantLock()

  private val continuationTokens: LinkedHashMap<String, State> = LinkedHashMap()
  private var latestContinuationToken = ""

  var queried = false

  /**
   * Get the latest continuation token to stream computations. Note: Also clear the
   * continuationTokenList for the incoming stream.
   *
   * @return the latest continuation token for the next stream.
   */
  suspend fun getLatestContinuationToken(): String {
    return if (!queried) {
      latestContinuationToken =
        continuationTokenClient
          .withWaitForReady()
          .getContinuationToken(getContinuationTokenRequest {})
          .token
      queried = true
      latestContinuationToken
    } else {
      lock.withLock {
        continuationTokens.clear()
        latestContinuationToken
      }
    }
  }

  /**
   * Add an UNPROCESSED continuation token entry into the list. The caller is responsible to
   * guarantee the order of insertion.
   */
  fun addPendingToken(continuationToken: String) {
    lock.withLock {
      latestContinuationToken = continuationToken
      continuationTokens.put(continuationToken, State.PENDING)
    }
  }

  /**
   * Mark a continuation token as PROCESSED. Update the latest continuation token to persistent
   * storage when all prior tokens are all PROCESSED.
   */
  suspend fun markTokenProcessed(token: String) {
    var lastProcessedToken = ""

    lock.withLock {
      continuationTokens[token] = State.PROCESSED

      for (item in continuationTokens) {
        if (item.value == State.PENDING) {
          break
        }
        lastProcessedToken = item.key
      }
    }
    if (lastProcessedToken != "") {
      // TODO(@renjiez): Throttle the calling of the api if needed.
      continuationTokenClient.setContinuationToken(
        setContinuationTokenRequest { this.token = lastProcessedToken }
      )
    }
  }
}
