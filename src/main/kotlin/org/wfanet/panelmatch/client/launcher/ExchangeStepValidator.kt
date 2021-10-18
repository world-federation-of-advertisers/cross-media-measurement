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
import org.wfanet.measurement.api.v2alpha.ExchangeStepKey
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.panelmatch.common.secrets.SecretMap

/** Indicates that an [ExchangeStep] is not valid to execute. */
class InvalidExchangeStepException(message: String) : Exception(message)

/** Determines whether an ExchangeStep is valid and can be safely executed. */
class ExchangeStepValidator(
  private val party: ExchangeWorkflow.Party,
  private val validExchangeWorkflows: SecretMap
) {
  /** Throws [InvalidExchangeStepException] if [exchangeStep] is invalid. */
  suspend fun validate(exchangeStep: ExchangeStep) {
    val serializedExchangeWorkflow = exchangeStep.serializedExchangeWorkflow
    val recurringExchangeId =
      requireNotNull(ExchangeStepKey.fromName(exchangeStep.name)).recurringExchangeId
    if (validExchangeWorkflows.get(recurringExchangeId) != serializedExchangeWorkflow) {
      throw InvalidExchangeStepException("Serialized ExchangeWorkflow unrecognized")
    }

    @Suppress("BlockingMethodInNonBlockingContext") // Proto parsing is lightweight
    val workflow = ExchangeWorkflow.parseFrom(serializedExchangeWorkflow)
    val step = workflow.getSteps(exchangeStep.stepIndex)
    if (step.party != party) {
      throw InvalidExchangeStepException("Party for step '${step.stepId}' was not ${party.name}")
    }
  }
}
