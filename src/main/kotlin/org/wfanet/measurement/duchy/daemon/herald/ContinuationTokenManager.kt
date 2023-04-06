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

import io.grpc.Status
import io.grpc.StatusException
import java.util.logging.Level
import java.util.logging.Logger
import org.wfanet.measurement.internal.duchy.ContinuationTokensGrpcKt.ContinuationTokensCoroutineStub
import org.wfanet.measurement.internal.duchy.getContinuationTokenRequest
import org.wfanet.measurement.internal.duchy.setContinuationTokenRequest

/** Manager for continuation tokens received along with Computations during streaming. */
class ContinuationTokenManager(
  private val continuationTokenClient: ContinuationTokensCoroutineStub
) {
  private enum class State {
    PENDING,
    PROCESSED,
  }

  private val continuationTokens: LinkedHashMap<String, State> = LinkedHashMap()
  private var latestContinuationToken = ""

  /**
   * Get the latest continuation token to stream computations.
   *
   * @return the latest continuation token.
   */
  suspend fun getLatestContinuationToken(): String {
    synchronized(this) {
      if (latestContinuationToken.isNotEmpty()) {
        return latestContinuationToken
      }
    }

    val response =
      continuationTokenClient
        .withWaitForReady()
        .getContinuationToken(getContinuationTokenRequest {})

    synchronized(this) {
      if (latestContinuationToken.isEmpty()) {
        latestContinuationToken = response.token
      }
      return latestContinuationToken
    }
  }

  /**
   * Add a continuationToken into [continuationTokens].
   *
   * The caller is responsible to guarantee insertions are consistent with the time order of the
   * received Computations.
   */
  @Synchronized
  fun addPendingToken(continuationToken: String) {
    continuationTokens[continuationToken] = State.PENDING
  }

  /**
   * Marks a [continuationToken] as processed.
   *
   * This will update the token returned by [getLatestContinuationToken] when all prior tokens in
   * sequence have been processed.
   */
  suspend fun markTokenProcessed(continuationToken: String) {
    val setRequest =
      synchronized(this) {
        continuationTokens[continuationToken] = State.PROCESSED

        val leadingProcessedTokens =
          continuationTokens
            .toList()
            .takeWhile { (_, state) -> state == State.PROCESSED }
            .map { (token, _) -> token }

        if (leadingProcessedTokens.isEmpty()) {
          return
        }
        latestContinuationToken = leadingProcessedTokens.last()

        // Remove leading processed tokens
        for (token in leadingProcessedTokens) {
          continuationTokens.remove(token)
        }

        setContinuationTokenRequest { token = latestContinuationToken }
      }

    // TODO(@renjiez): Throttle the calling of the api if needed.
    try {
      continuationTokenClient.setContinuationToken(setRequest)
    } catch (e: StatusException) {
      if (e.status.code == Status.Code.FAILED_PRECONDITION) {
        logger.log(Level.WARNING, e) { "Failure happened during setContinuationToken" }
      } else {
        throw SetContinuationTokenException(e.message ?: "Exception during setContinuationToken")
      }
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}

class SetContinuationTokenException(message: String) : Exception(message)
