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

package org.wfanet.measurement.api.v2alpha

import org.wfanet.measurement.common.ResourceNameParser
import org.wfanet.measurement.common.api.ResourceKey

private val parsers =
  listOf(
    ResourceNameParser("recurringExchanges/{recurring_exchange}/exchanges/{exchange}"),
    ResourceNameParser(
      "dataProviders/{data_provider}/recurringExchanges/{recurring_exchange}/exchanges/{exchange}"
    ),
    ResourceNameParser(
      "modelProviders/{model_provider}/recurringExchanges/{recurring_exchange}/exchanges/{exchange}"
    )
  )

/** [ExchangeKey] of an Exchange. */
data class ExchangeKey(
  val dataProviderId: String?,
  val modelProviderId: String?,
  val recurringExchangeId: String,
  val exchangeId: String
) : ResourceKey {
  init {
    require((dataProviderId == null) || (modelProviderId == null))
  }

  override fun toName(): String {
    return parsers
      .first()
      .assembleName(
        mapOf(
          IdVariable.RECURRING_EXCHANGE to recurringExchangeId,
          IdVariable.EXCHANGE to exchangeId
        )
      )
  }

  companion object FACTORY : ResourceKey.Factory<ExchangeKey> {
    val defaultValue = ExchangeKey(null, null, "", "")

    override fun fromName(resourceName: String): ExchangeKey? {
      for (parser in parsers) {
        val idVars = parser.parseIdVars(resourceName) ?: continue
        return ExchangeKey(
          idVars[IdVariable.DATA_PROVIDER],
          idVars[IdVariable.MODEL_PROVIDER],
          idVars.getValue(IdVariable.RECURRING_EXCHANGE),
          idVars.getValue(IdVariable.EXCHANGE)
        )
      }
      return null
    }
  }
}
