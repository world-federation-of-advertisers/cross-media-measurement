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

import org.wfanet.measurement.api.v2alpha.ExchangeStep
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttempt
import org.wfanet.measurement.kingdom.service.api.v2alpha.ExchangeStepAttemptKey

/** Finds an [ExchangeStep], validates it, and starts executing the work. */
class ExchangeStepLauncher(
  private val apiClient: ApiClient,
  private val validator: ExchangeStepValidator,
  private val jobLauncher: JobLauncher
) {

  /**
   * Finds a single ready Exchange Step and starts executing. If an Exchange Step is found,
   * validates it, and starts executing. If not found simply returns.
   */
  suspend fun findAndRunExchangeStep() {
    val (exchangeStep, attemptKey) = apiClient.claimExchangeStep() ?: return

    try {
      validator.validate(exchangeStep)
    } catch (e: InvalidExchangeStepException) {
      invalidateAttempt(attemptKey, e)
      return
    }

    jobLauncher.execute(exchangeStep, attemptKey)
  }

  private suspend fun invalidateAttempt(attemptKey: ExchangeStepAttemptKey, cause: Throwable) {
    // TODO: log an error or retry a few times if this fails.
    // TODO: add API-level support for some type of justification about what went wrong.
    apiClient.finishExchangeStepAttempt(
      attemptKey,
      ExchangeStepAttempt.State.FAILED_STEP,
      listOf(cause.message!!)
    )
  }
}
