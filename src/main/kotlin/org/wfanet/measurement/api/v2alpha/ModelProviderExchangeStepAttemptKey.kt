/*
 * Copyright 2023 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.api.v2alpha

import org.wfanet.measurement.common.ResourceNameParser
import org.wfanet.measurement.common.api.ResourceKey

/** [ResourceKey] for an ExchangeStepAttempt with a ModelProvider RecurringExchange parent. */
data class ModelProviderExchangeStepAttemptKey(
  override val parentKey: ModelProviderExchangeStepKey,
  override val exchangeStepAttemptId: String,
) : ExchangeStepAttemptKey {
  constructor(
    modelProviderId: String,
    recurringExchangeId: String,
    exchangeId: String,
    exchangeStepId: String,
    exchangeStepAttemptId: String,
  ) : this(
    ModelProviderExchangeStepKey(modelProviderId, recurringExchangeId, exchangeId, exchangeStepId),
    exchangeStepAttemptId,
  )

  val modelProviderId: String
    get() = parentKey.modelProviderId

  override fun toName(): String {
    return parser.assembleName(
      mapOf(
        IdVariable.MODEL_PROVIDER to modelProviderId,
        IdVariable.RECURRING_EXCHANGE to recurringExchangeId,
        IdVariable.EXCHANGE to exchangeId,
        IdVariable.EXCHANGE_STEP to exchangeStepId,
        IdVariable.EXCHANGE_STEP_ATTEMPT to exchangeStepAttemptId,
      )
    )
  }

  companion object FACTORY : ResourceKey.Factory<ModelProviderExchangeStepAttemptKey> {
    const val PATTERN = "${ModelProviderExchangeStepKey.PATTERN}/attempts/{exchange_step_attempt}"
    private val parser = ResourceNameParser(PATTERN)

    override fun fromName(resourceName: String): ModelProviderExchangeStepAttemptKey? {
      val idVars: Map<IdVariable, String> = parser.parseIdVars(resourceName) ?: return null
      return ModelProviderExchangeStepAttemptKey(
        idVars.getValue(IdVariable.MODEL_PROVIDER),
        idVars.getValue(IdVariable.RECURRING_EXCHANGE),
        idVars.getValue(IdVariable.EXCHANGE),
        idVars.getValue(IdVariable.EXCHANGE_STEP),
        idVars.getValue(IdVariable.EXCHANGE_STEP_ATTEMPT),
      )
    }
  }
}
