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

/** [ResourceKey] for an Exchange with a ModelProvider RecurringExchange parent. */
data class ModelProviderExchangeKey(
  override val parentKey: ModelProviderRecurringExchangeKey,
  override val exchangeId: String,
) : ExchangeKey {
  constructor(
    modelProviderId: String,
    recurringExchangeId: String,
    exchangeId: String,
  ) : this(ModelProviderRecurringExchangeKey(modelProviderId, recurringExchangeId), exchangeId)

  val modelProviderId: String
    get() = parentKey.modelProviderId

  override fun toName(): String {
    return parser.assembleName(
      mapOf(
        IdVariable.MODEL_PROVIDER to modelProviderId,
        IdVariable.RECURRING_EXCHANGE to recurringExchangeId,
        IdVariable.EXCHANGE to exchangeId,
      )
    )
  }

  companion object FACTORY : ResourceKey.Factory<ModelProviderExchangeKey> {
    const val PATTERN = "${ModelProviderRecurringExchangeKey.PATTERN}/exchanges/{exchange}"
    private val parser = ResourceNameParser(PATTERN)

    override fun fromName(resourceName: String): ModelProviderExchangeKey? {
      val idVars: Map<IdVariable, String> = parser.parseIdVars(resourceName) ?: return null
      return ModelProviderExchangeKey(
        idVars.getValue(IdVariable.MODEL_PROVIDER),
        idVars.getValue(IdVariable.RECURRING_EXCHANGE),
        idVars.getValue(IdVariable.EXCHANGE),
      )
    }
  }
}
