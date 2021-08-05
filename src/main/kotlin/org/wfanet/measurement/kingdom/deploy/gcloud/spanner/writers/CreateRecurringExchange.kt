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
import org.wfanet.measurement.gcloud.spanner.bufferTo
import org.wfanet.measurement.gcloud.spanner.insertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.setJson
import org.wfanet.measurement.internal.kingdom.RecurringExchange
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.DataProviderReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelProviderReader

private val INITIAL_STATE: RecurringExchange.State = RecurringExchange.State.ACTIVE

class CreateRecurringExchange(private val recurringExchange: RecurringExchange) :
  SimpleSpannerWriter<RecurringExchange>() {
  override suspend fun TransactionScope.runTransaction(): RecurringExchange {
    val externalDataProviderId =
      DataProviderReader()
        .readExternalId(transactionContext, ExternalId(recurringExchange.externalDataProviderId))
        .dataProviderId

    val externalModelProviderId =
      ModelProviderReader()
        .readExternalId(transactionContext, ExternalId(recurringExchange.externalModelProviderId))
        .modelProviderId

    val externalId = idGenerator.generateExternalId()
    insertMutation("RecurringExchanges") {
        set("RecurringExchangeId" to idGenerator.generateInternalId().value)
        set("ExternalRecurringExchangeId" to externalId.value)
        set("ModelProviderId" to externalModelProviderId)
        set("DataProviderId" to externalDataProviderId)
        set("State" to INITIAL_STATE)
        set("NextExchangeDate" to recurringExchange.nextExchangeDate.toCloudDate())
        set("RecurringExchangeDetails" to recurringExchange.details)
        setJson("RecurringExchangeDetailsJson" to recurringExchange.details)
      }
      .bufferTo(transactionContext)

    return recurringExchange
      .toBuilder()
      .apply {
        externalRecurringExchangeId = externalId.value
        state = INITIAL_STATE
      }
      .build()
  }
}
