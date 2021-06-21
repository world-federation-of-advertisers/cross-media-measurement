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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import org.wfanet.measurement.common.ResourceNameParser

private val parser =
  ResourceNameParser("recurringExchanges/{recurring_exchange}/exchanges/{exchange}")

/** [ExchangeKey] of an Exchange. */
data class ExchangeKey(val recurringExchangeId: String, val exchangeId: String) : ResourceKey {
  override fun toName(): String {
    return parser.assembleName(
      mapOf(IdVariable.RECURRING_EXCHANGE to recurringExchangeId, IdVariable.EXCHANGE to exchangeId)
    )
  }

  companion object {
    val defaultValue = ExchangeKey("", "")

    fun fromName(resourceName: String): ExchangeKey? {
      return parser.parseIdVars(resourceName)?.let {
        ExchangeKey(it.getValue(IdVariable.RECURRING_EXCHANGE), it.getValue(IdVariable.EXCHANGE))
      }
    }
  }
}
