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
import org.wfanet.panelmatch.common.loggerFor

/** Finds an [ExchangeStep], validates it, and starts executing the work. */
class ExchangeStepLauncher(
  private val apiClient: ApiClient,
  private val taskLauncher: ExchangeStepExecutor,
) {
  /**
   * Finds a single ready Exchange Step and starts executing. If an Exchange Step is found, this
   * passes it to the executor for validation and execution. If not found simply returns.
   */
  suspend fun findAndRunExchangeStep() {
    try {
      apiClient.claimExchangeStep()?.let {
        taskLauncher.execute(it.exchangeStep, it.exchangeStepAttempt)
      }
    } catch (e: Exception) {
      logger.log(Level.SEVERE, e) { "Exchange Launcher Error." }
    }
  }

  companion object {
    private val logger by loggerFor()
  }
}
