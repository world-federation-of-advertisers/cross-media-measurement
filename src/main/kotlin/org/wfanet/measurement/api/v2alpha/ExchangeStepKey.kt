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

/** [ResourceKey] of an ExchangeStep. */
sealed interface ExchangeStepKey : ChildResourceKey {
  override val parentKey: ExchangeKey

  val recurringExchangeId: String
    get() = parentKey.recurringExchangeId

  val exchangeId: String
    get() = parentKey.exchangeId

  val exchangeStepId: String

  companion object FACTORY : ResourceKey.Factory<ExchangeStepKey> {
    override fun fromName(resourceName: String): ExchangeStepKey? {
      return CanonicalExchangeStepKey.fromName(resourceName)
        ?: DataProviderExchangeStepKey.fromName(resourceName)
        ?: ModelProviderExchangeStepKey.fromName(resourceName)
    }
  }
}

/** Canonical [ResourceKey] of an ExchangeStep */
data class CanonicalExchangeStepKey(
  override val parentKey: CanonicalExchangeKey,
  override val exchangeStepId: String,
) : ExchangeStepKey {
  constructor(
    recurringExchangeId: String,
    exchangeId: String,
    exchangeStepId: String,
  ) : this(CanonicalExchangeKey(recurringExchangeId, exchangeId), exchangeStepId)

  override fun toName(): String {
    return parser.assembleName(
      mapOf(
        IdVariable.RECURRING_EXCHANGE to recurringExchangeId,
        IdVariable.EXCHANGE to exchangeId,
        IdVariable.EXCHANGE_STEP to exchangeStepId,
      )
    )
  }

  companion object FACTORY : ResourceKey.Factory<CanonicalExchangeStepKey> {
    private val parser =
      ResourceNameParser(
        "recurringExchanges/{recurring_exchange}/exchanges/{exchange}/steps/{exchange_step}"
      )
    val defaultValue = CanonicalExchangeStepKey("", "", "")

    override fun fromName(resourceName: String): CanonicalExchangeStepKey? {
      return parser.parseIdVars(resourceName)?.let {
        CanonicalExchangeStepKey(
          it.getValue(IdVariable.RECURRING_EXCHANGE),
          it.getValue(IdVariable.EXCHANGE),
          it.getValue(IdVariable.EXCHANGE_STEP),
        )
      }
    }
  }
}
