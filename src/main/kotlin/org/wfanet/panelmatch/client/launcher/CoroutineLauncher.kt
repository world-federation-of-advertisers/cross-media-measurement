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

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.plus
import org.wfanet.measurement.api.v2alpha.ExchangeStep
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptKey
import org.wfanet.panelmatch.client.launcher.ExchangeStepValidator.ValidatedExchangeStep

/** Executes an [ExchangeStep] in a new coroutine in [scope]. */
class CoroutineLauncher(
  private val scope: CoroutineScope = CoroutineScope(Dispatchers.Default),
  private val stepExecutor: ExchangeStepExecutor
) : JobLauncher {
  override suspend fun execute(step: ValidatedExchangeStep, attemptKey: ExchangeStepAttemptKey) {
    (scope + SupervisorJob()).launch(CoroutineName(attemptKey.toName())) {
      stepExecutor.execute(step, attemptKey)
    }
  }
}
