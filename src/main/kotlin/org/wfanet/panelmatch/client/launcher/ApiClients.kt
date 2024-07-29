// Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.panelmatch.client.launcher

import kotlinx.coroutines.sync.Semaphore
import org.wfanet.panelmatch.client.common.ExchangeStepAttemptKey
import org.wfanet.panelmatch.client.internal.ExchangeStepAttempt

/**
 * Returns a new [ApiClient] that delegates to this client, but enforces that no more than
 * [maxParallelClaimedExchangeSteps] steps are claimed at any given time. If this limit would be
 * exceeded, then attempting to claim a new exchange step instead returns null immediately.
 */
fun ApiClient.withMaxParallelClaimedExchangeSteps(maxParallelClaimedExchangeSteps: Int): ApiClient {
  require(maxParallelClaimedExchangeSteps > 0) {
    "maxParallelClaimedExchangeSteps must be at least 1, but was $maxParallelClaimedExchangeSteps"
  }
  return ApiClientWithMaxParallelClaimedExchangeSteps(
    this,
    Semaphore(permits = maxParallelClaimedExchangeSteps),
  )
}

private class ApiClientWithMaxParallelClaimedExchangeSteps(
  private val delegate: ApiClient,
  private val semaphore: Semaphore,
) : ApiClient by delegate {

  override suspend fun claimExchangeStep(): ApiClient.ClaimedExchangeStep? {
    val acquired = semaphore.tryAcquire()
    if (!acquired) {
      return null
    }
    val step =
      try {
        delegate.claimExchangeStep()
      } catch (e: Exception) {
        semaphore.release()
        throw e
      }
    if (step == null) {
      semaphore.release()
    }
    return step
  }

  override suspend fun finishExchangeStepAttempt(
    key: ExchangeStepAttemptKey,
    finalState: ExchangeStepAttempt.State,
    logEntryMessages: Iterable<String>,
  ) {
    semaphore.release()
    delegate.finishExchangeStepAttempt(key, finalState, logEntryMessages)
  }
}
