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

/** [ResourceKey] of an ExchangeStepAttempt. */
sealed interface ExchangeStepAttemptKey : ChildResourceKey {
  override val parentKey: ExchangeStepKey

  val recurringExchangeId: String
    get() = parentKey.recurringExchangeId

  val exchangeId: String
    get() = parentKey.exchangeId

  val exchangeStepId: String
    get() = parentKey.exchangeStepId

  val exchangeStepAttemptId: String

  companion object FACTORY : ResourceKey.Factory<ExchangeStepAttemptKey> {
    override fun fromName(resourceName: String): ExchangeStepAttemptKey? {
      return CanonicalExchangeStepAttemptKey.fromName(resourceName)
        ?: DataProviderExchangeStepAttemptKey.fromName(resourceName)
        ?: ModelProviderExchangeStepAttemptKey.fromName(resourceName)
    }
  }
}

/** Canonical [ResourceKey] of an ExchangeStepAttempt. */
data class CanonicalExchangeStepAttemptKey(
  override val parentKey: CanonicalExchangeStepKey,
  override val exchangeStepAttemptId: String,
) : ExchangeStepAttemptKey {
  constructor(
    recurringExchangeId: String,
    exchangeId: String,
    exchangeStepId: String,
    exchangeStepAttemptId: String,
  ) : this(
    CanonicalExchangeStepKey(recurringExchangeId, exchangeId, exchangeStepId),
    exchangeStepAttemptId,
  )

  override fun toName(): String {
    return parser.assembleName(
      mapOf(
        IdVariable.RECURRING_EXCHANGE to recurringExchangeId,
        IdVariable.EXCHANGE to exchangeId,
        IdVariable.EXCHANGE_STEP to exchangeStepId,
        IdVariable.EXCHANGE_STEP_ATTEMPT to exchangeStepAttemptId,
      )
    )
  }

  companion object FACTORY : ResourceKey.Factory<CanonicalExchangeStepAttemptKey> {
    private val parser =
      ResourceNameParser(
        "recurringExchanges/{recurring_exchange}/exchanges/{exchange}/steps/{exchange_step}/" +
          "attempts/{exchange_step_attempt}"
      )

    val defaultValue = CanonicalExchangeStepAttemptKey("", "", "", "")

    override fun fromName(resourceName: String): CanonicalExchangeStepAttemptKey? {
      return parser.parseIdVars(resourceName)?.let {
        CanonicalExchangeStepAttemptKey(
          it.getValue(IdVariable.RECURRING_EXCHANGE),
          it.getValue(IdVariable.EXCHANGE),
          it.getValue(IdVariable.EXCHANGE_STEP),
          it.getValue(IdVariable.EXCHANGE_STEP_ATTEMPT),
        )
      }
    }
  }
}
