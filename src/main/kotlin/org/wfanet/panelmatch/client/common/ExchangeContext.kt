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

package org.wfanet.panelmatch.client.common

import java.time.LocalDate
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptKey
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.panelmatch.common.ExchangeDateKey

/** Contextual information about an ExchangeStepAttempt. */
data class ExchangeContext(
  val attemptKey: ExchangeStepAttemptKey,
  val date: LocalDate,
  val workflow: ExchangeWorkflow,
  val step: ExchangeWorkflow.Step
) {
  val recurringExchangeId: String
    get() = attemptKey.recurringExchangeId

  val partnerName: String by lazy {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    when (step.party) {
      ExchangeWorkflow.Party.MODEL_PROVIDER -> workflow.exchangeIdentifiers.dataProvider
      ExchangeWorkflow.Party.DATA_PROVIDER -> workflow.exchangeIdentifiers.modelProvider
      ExchangeWorkflow.Party.PARTY_UNSPECIFIED, ExchangeWorkflow.Party.UNRECOGNIZED ->
        error("Invalid step: $step")
    }
  }

  val exchangeDateKey: ExchangeDateKey by lazy {
    ExchangeDateKey(attemptKey.recurringExchangeId, date)
  }

  override fun toString(): String {
    return "ExchangeContext(step=${step.stepId}, attempt=${attemptKey.toName()})"
  }
}
