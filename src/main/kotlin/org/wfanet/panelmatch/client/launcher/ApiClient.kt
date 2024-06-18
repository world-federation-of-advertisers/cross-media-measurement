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

import com.google.protobuf.ByteString
import java.time.LocalDate
import org.wfanet.panelmatch.client.common.ExchangeStepAttemptKey
import org.wfanet.panelmatch.client.internal.ExchangeStepAttempt
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow

/** Abstracts interactions with the centralized Panel Match APIs. */
interface ApiClient {
  data class ClaimedExchangeStep(
    val attemptKey: ExchangeStepAttemptKey,
    val exchangeDate: LocalDate,
    val stepIndex: Int,
    val workflow: ExchangeWorkflow,
    val workflowFingerprint: ByteString,
  )

  /**
   * Attempts to fetch a [ClaimedExchangeStep] to work on.
   *
   * @return an unvalidated [ClaimedExchangeStep] ready to work on -- or null if none exist.
   */
  suspend fun claimExchangeStep(): ClaimedExchangeStep?

  /** Attaches debug log entries to an [ExchangeStepAttempt] given its [key]. */
  suspend fun appendLogEntry(key: ExchangeStepAttemptKey, messages: Iterable<String>)

  /**
   * Marks the [ExchangeStepAttempt] identified by [key] as complete (successfully or otherwise).
   */
  suspend fun finishExchangeStepAttempt(
    key: ExchangeStepAttemptKey,
    finalState: ExchangeStepAttempt.State,
    logEntryMessages: Iterable<String> = emptyList(),
  )
}
