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

package org.wfanet.panelmatch.client.exchangetasks

import org.wfanet.panelmatch.client.internal.ExchangeStepAttempt.State

/**
 * Exception to be thrown by exchange tasks upon failure. Allows the task to specify [attemptState],
 * for example if the attempt should result in a permanent step failure.
 */
class ExchangeTaskFailedException private constructor(cause: Throwable, val attemptState: State) :
  Exception(cause) {
  companion object {
    /**
     * Returns an [ExchangeTaskFailedException] with a transient [attemptState], allowing the step
     * to retry.
     */
    fun ofTransient(cause: Throwable): ExchangeTaskFailedException =
      ExchangeTaskFailedException(cause, State.FAILED)

    /**
     * Returns an [ExchangeTaskFailedException] with a permanent [attemptState], which will mark the
     * entire exchange as failed.
     */
    fun ofPermanent(cause: Throwable): ExchangeTaskFailedException =
      ExchangeTaskFailedException(cause, State.FAILED_STEP)
  }
}
