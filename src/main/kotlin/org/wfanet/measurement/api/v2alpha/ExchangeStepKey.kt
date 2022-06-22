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

private val parser =
  ResourceNameParser(
    "recurringExchanges/{recurring_exchange}/exchanges/{exchange}/steps/{exchange_step}"
  )

/** [ExchangeStepKey] of an Exchange Step. */
data class ExchangeStepKey(
  val recurringExchangeId: String,
  val exchangeId: String,
  val exchangeStepId: String
) : ResourceKey {
  override fun toName(): String {
    return parser.assembleName(
      mapOf(
        IdVariable.RECURRING_EXCHANGE to recurringExchangeId,
        IdVariable.EXCHANGE to exchangeId,
        IdVariable.EXCHANGE_STEP to exchangeStepId
      )
    )
  }

  companion object FACTORY : ResourceKey.Factory<ExchangeStepKey> {
    val defaultValue = ExchangeStepKey("", "", "")

    override fun fromName(resourceName: String): ExchangeStepKey? {
      return parser.parseIdVars(resourceName)?.let {
        ExchangeStepKey(
          it.getValue(IdVariable.RECURRING_EXCHANGE),
          it.getValue(IdVariable.EXCHANGE),
          it.getValue(IdVariable.EXCHANGE_STEP)
        )
      }
    }
  }
}
