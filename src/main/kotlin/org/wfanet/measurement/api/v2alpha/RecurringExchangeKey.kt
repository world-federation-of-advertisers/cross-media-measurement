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

/** [ResourceKey] of a RecurringExchange. */
sealed interface RecurringExchangeKey : ResourceKey {
  val recurringExchangeId: String
}

/** [ResourceKey] of the parent of a [RecurringExchange] */
sealed interface RecurringExchangeParentKey : ResourceKey {
  companion object FACTORY : ResourceKey.Factory<RecurringExchangeParentKey> {
    override fun fromName(resourceName: String): RecurringExchangeParentKey? {
      return DataProviderKey.fromName(resourceName) ?: ModelProviderKey.fromName(resourceName)
    }
  }
}

/** Canonical [ResourceKey] of a RecurringExchange. */
data class CanonicalRecurringExchangeKey(override val recurringExchangeId: String) :
  RecurringExchangeKey {
  override fun toName(): String {
    return parser.assembleName(mapOf(IdVariable.RECURRING_EXCHANGE to recurringExchangeId))
  }

  companion object FACTORY : ResourceKey.Factory<CanonicalRecurringExchangeKey> {
    const val PATTERN = "recurringExchanges/{recurring_exchange}"
    private val parser = ResourceNameParser(PATTERN)
    val defaultValue = CanonicalRecurringExchangeKey("")

    override fun fromName(resourceName: String): CanonicalRecurringExchangeKey? {
      return parser.parseIdVars(resourceName)?.let {
        CanonicalRecurringExchangeKey(it.getValue(IdVariable.RECURRING_EXCHANGE))
      }
    }
  }
}
