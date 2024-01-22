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
import org.wfanet.measurement.common.api.ChildResourceKey
import org.wfanet.measurement.common.api.ResourceKey

/** [ResourceKey] of an Exchange. */
sealed interface ExchangeKey : ChildResourceKey {
  override val parentKey: RecurringExchangeKey

  val recurringExchangeId: String
    get() = parentKey.recurringExchangeId

  val exchangeId: String

  companion object FACTORY : ResourceKey.Factory<ExchangeKey> {
    override fun fromName(resourceName: String): ExchangeKey? {
      return CanonicalExchangeKey.fromName(resourceName)
        ?: DataProviderExchangeKey.fromName(resourceName)
        ?: ModelProviderExchangeKey.fromName(resourceName)
    }
  }
}

/** Canonical [ResourceKey] of an Exchange. */
data class CanonicalExchangeKey(
  override val parentKey: CanonicalRecurringExchangeKey,
  override val exchangeId: String,
) : ExchangeKey {
  constructor(
    recurringExchangeId: String,
    exchangeId: String,
  ) : this(CanonicalRecurringExchangeKey(recurringExchangeId), exchangeId)

  override fun toName(): String {
    return parser.assembleName(
      mapOf(IdVariable.RECURRING_EXCHANGE to recurringExchangeId, IdVariable.EXCHANGE to exchangeId)
    )
  }

  companion object FACTORY : ResourceKey.Factory<CanonicalExchangeKey> {
    private val parser =
      ResourceNameParser("${CanonicalRecurringExchangeKey.PATTERN}/exchanges/{exchange}")

    val defaultValue = CanonicalExchangeKey("", "")

    override fun fromName(resourceName: String): CanonicalExchangeKey? {
      val idVars = parser.parseIdVars(resourceName) ?: return null
      return CanonicalExchangeKey(
        idVars.getValue(IdVariable.RECURRING_EXCHANGE),
        idVars.getValue(IdVariable.EXCHANGE),
      )
    }
  }
}
