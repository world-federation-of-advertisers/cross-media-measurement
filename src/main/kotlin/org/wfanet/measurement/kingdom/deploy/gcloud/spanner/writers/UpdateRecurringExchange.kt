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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.type.Date
import org.wfanet.measurement.gcloud.common.toCloudDate
import org.wfanet.measurement.gcloud.spanner.bufferTo
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.setJson
import org.wfanet.measurement.gcloud.spanner.updateMutation
import org.wfanet.measurement.internal.kingdom.RecurringExchange

class UpdateRecurringExchange(
  private val recurringExchange: RecurringExchange,
  private val recurringExchangeId: Long,
  private val nextExchangeDate: Date,
  private val state: RecurringExchange.State
) : SimpleSpannerWriter<RecurringExchange>() {
  override suspend fun TransactionScope.runTransaction(): RecurringExchange {
    if (recurringExchange.state == state && recurringExchange.nextExchangeDate == nextExchangeDate
    ) {
      return recurringExchange
    }

    require(!recurringExchange.state.isTerminal) {
      "RecurringExchange: $recurringExchange is in a terminal state."
    }
    updateMutation("RecurringExchanges") {
        set("RecurringExchangeId" to recurringExchangeId)
        set("ExternalRecurringExchangeId" to recurringExchange.externalRecurringExchangeId)
        set("State" to state)
        set("NextExchangeDate" to nextExchangeDate.toCloudDate())
        set("RecurringExchangeDetails" to recurringExchange.details)
        setJson("RecurringExchangeDetailsJson" to recurringExchange.details)
      }
      .bufferTo(transactionContext)

    return recurringExchange
      .toBuilder()
      .setNextExchangeDate(nextExchangeDate)
      .setState(state)
      .build()
  }

  private val RecurringExchange.State.isTerminal: Boolean
    get() =
      when (this) {
        RecurringExchange.State.ACTIVE -> false
        RecurringExchange.State.STATE_UNSPECIFIED,
        RecurringExchange.State.UNRECOGNIZED,
        RecurringExchange.State.RETIRED -> true
      }
}
