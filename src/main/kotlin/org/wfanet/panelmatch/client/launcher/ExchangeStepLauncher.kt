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
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.sync.Semaphore
import org.wfanet.measurement.api.v2alpha.ExchangeStep
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttempt
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptKey
import org.wfanet.panelmatch.client.launcher.InvalidExchangeStepException.FailureType.PERMANENT
import org.wfanet.panelmatch.client.launcher.InvalidExchangeStepException.FailureType.TRANSIENT
import org.wfanet.panelmatch.common.loggerFor

/** Finds an [ExchangeStep], validates it, and starts executing the work. */
class ExchangeStepLauncher(
  private val apiClient: ApiClient,
  private val taskLauncher: ExchangeStepExecutor,
  maxTaskCoroutines: Int? = null
) {
  private val semaphore = if (maxTaskCoroutines !== null) Semaphore(maxTaskCoroutines) else null

  /**
   * Finds a single ready Exchange Step and starts executing. If an Exchange Step is found, this
   * passes it to the executor for validation and execution. If not found simply returns.
   *
   * If [maxTaskCoroutines] is set, this will try to acquire a permit before claiming a task. If it
   * is unable to, it will return before even trying to claim an exchange step.
   */
  suspend fun findAndRunExchangeStep() {
    if(semaphore == null || semaphore.tryAcquire()) {
      val numPermits = semaphore?.availablePermits ?: "unlimited"
      logger.log(
        Level.INFO,
        "Claiming an Exchange Step, $numPermits task permits remain.")
      val (exchangeStep, attemptKey) = apiClient.claimExchangeStep() ?: return
      taskLauncher.execute(exchangeStep, attemptKey)
      semaphore?.release()
    } else {
      logger.log(Level.INFO, "No available permits to claim and run a task.")
    }
  }

  companion object {
    private val logger by loggerFor()
  }
}
