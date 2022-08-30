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

import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.common.toCloudDate
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.setJson
import org.wfanet.measurement.internal.kingdom.Exchange
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.RecurringExchangeNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.RecurringExchangeReader

private val INITIAL_STATE: Exchange.State = Exchange.State.ACTIVE

/**
 * [SpannerWriter] for creating an [Exchange].
 *
 * Throws one of the following [KingdomInternalException] types on [execute]:
 * * [RecurringExchangeNotFoundException]
 */
class CreateExchange(private val exchange: Exchange) : SimpleSpannerWriter<Exchange>() {
  override suspend fun TransactionScope.runTransaction(): Exchange {
    val externalRecurringExchangeId = ExternalId(exchange.externalRecurringExchangeId)
    val recurringExchangeId =
      RecurringExchangeReader()
        .readByExternalRecurringExchangeId(transactionContext, externalRecurringExchangeId)
        ?.recurringExchangeId
        ?: throw RecurringExchangeNotFoundException(externalRecurringExchangeId)

    transactionContext.bufferInsertMutation("Exchanges") {
      set("RecurringExchangeId" to recurringExchangeId)
      set("Date" to exchange.date.toCloudDate())
      set("State" to INITIAL_STATE)
      set("ExchangeDetails" to exchange.details)
      setJson("ExchangeDetailsJson" to exchange.details)
    }

    return exchange.toBuilder().apply { state = INITIAL_STATE }.build()
  }
}
