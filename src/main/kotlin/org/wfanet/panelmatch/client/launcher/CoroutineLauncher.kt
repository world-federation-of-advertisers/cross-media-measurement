// Copyright 2021 The Cross-Media Measurement Authors
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

import java.util.logging.Level
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.plus
import kotlinx.coroutines.sync.Semaphore
import org.wfanet.measurement.api.v2alpha.ExchangeStep
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttempt
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptKey
import org.wfanet.panelmatch.client.logger.addToTaskLog
import org.wfanet.panelmatch.client.launcher.ExchangeStepValidator.ValidatedExchangeStep
import org.wfanet.panelmatch.common.loggerFor

/** Executes an [ExchangeStep] in a new coroutine in [scope]. */
class CoroutineLauncher(
  private val scope: CoroutineScope = CoroutineScope(Dispatchers.Default),
  private val stepExecutor: ExchangeStepExecutor,
  private val apiClient: ApiClient? = null,
  maxCoroutines: Int? = null
) : JobLauncher {
  private val semaphore = if (maxCoroutines !== null) Semaphore(maxCoroutines) else null

  override suspend fun execute(step: ValidatedExchangeStep, attemptKey: ExchangeStepAttemptKey) {
    (scope + SupervisorJob()).launch(CoroutineName(attemptKey.toName())) {
      try {
        if (semaphore !== null) semaphore.acquire()
        stepExecutor.execute(step, attemptKey)
      } catch(e: Exception) {
        invalidateAttempt(attemptKey, e)
      } finally {
        if (semaphore !== null) semaphore.release()
      }
    }
  }

  private suspend fun invalidateAttempt(attemptKey: ExchangeStepAttemptKey, exception: Exception) {
    logger.addToTaskLog("Exception caught in CoroutineLauncher.execute().")
    logger.addToTaskLog(exception, Level.SEVERE)

    val state =
      when (exception) {
        is InvalidExchangeStepException ->
          when (exception.type) {
            InvalidExchangeStepException.FailureType.PERMANENT -> ExchangeStepAttempt.State.FAILED_STEP
            InvalidExchangeStepException.FailureType.TRANSIENT -> ExchangeStepAttempt.State.FAILED
          }
        else -> ExchangeStepAttempt.State.FAILED
      }

    // TODO: log an error or retry a few times if this fails.
    // TODO: add API-level support for some type of justification about what went wrong.
    apiClient?.finishExchangeStepAttempt(attemptKey, state, listOfNotNull(exception.message))
  }

  companion object {
    private val logger by loggerFor()
  }
}
