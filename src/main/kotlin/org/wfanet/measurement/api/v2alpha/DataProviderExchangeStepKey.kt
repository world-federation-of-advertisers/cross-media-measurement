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

/** [ResourceKey] for an ExchangeStep with a DataProvider RecurringExchange parent. */
data class DataProviderExchangeStepKey(
  override val parentKey: DataProviderExchangeKey,
  override val exchangeStepId: String,
) : ExchangeStepKey {
  constructor(
    dataProviderId: String,
    recurringExchangeId: String,
    exchangeId: String,
    exchangeStepId: String,
  ) : this(DataProviderExchangeKey(dataProviderId, recurringExchangeId, exchangeId), exchangeStepId)

  val dataProviderId: String
    get() = parentKey.dataProviderId

  override fun toName(): String {
    return parser.assembleName(
      mapOf(
        IdVariable.DATA_PROVIDER to dataProviderId,
        IdVariable.RECURRING_EXCHANGE to recurringExchangeId,
        IdVariable.EXCHANGE to exchangeId,
        IdVariable.EXCHANGE_STEP to exchangeStepId,
      )
    )
  }

  companion object FACTORY : ResourceKey.Factory<DataProviderExchangeStepKey> {
    const val PATTERN = "${DataProviderExchangeKey.PATTERN}/steps/{exchange_step}"
    private val parser = ResourceNameParser(PATTERN)

    override fun fromName(resourceName: String): DataProviderExchangeStepKey? {
      val idVars: Map<IdVariable, String> = parser.parseIdVars(resourceName) ?: return null
      return DataProviderExchangeStepKey(
        idVars.getValue(IdVariable.DATA_PROVIDER),
        idVars.getValue(IdVariable.RECURRING_EXCHANGE),
        idVars.getValue(IdVariable.EXCHANGE),
        idVars.getValue(IdVariable.EXCHANGE_STEP),
      )
    }
  }
}
