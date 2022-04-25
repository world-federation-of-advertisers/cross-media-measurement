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

import io.grpc.Status
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.common.toCloudDate
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.setJson
import org.wfanet.measurement.internal.kingdom.RecurringExchange
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.DataProviderReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelProviderReader

private val INITIAL_STATE: RecurringExchange.State = RecurringExchange.State.ACTIVE

class CreateRecurringExchange(private val recurringExchange: RecurringExchange) :
    SimpleSpannerWriter<RecurringExchange>() {
  override suspend fun TransactionScope.runTransaction(): RecurringExchange {
    val dataProviderId =
        DataProviderReader()
            .readByExternalDataProviderId(
                transactionContext, ExternalId(recurringExchange.externalDataProviderId))
            ?.dataProviderId
            ?: failGrpc(Status.NOT_FOUND) { "DataProvider not found" }

    val modelProviderId =
        ModelProviderReader()
            .readByExternalModelProviderId(
                transactionContext, ExternalId(recurringExchange.externalModelProviderId))
            ?.modelProviderId
            ?: failGrpc(Status.NOT_FOUND) { "ModelProvider not found" }

    val externalId = idGenerator.generateExternalId()
    transactionContext.bufferInsertMutation("RecurringExchanges") {
      set("RecurringExchangeId" to idGenerator.generateInternalId().value)
      set("ExternalRecurringExchangeId" to externalId)
      set("ModelProviderId" to modelProviderId)
      set("DataProviderId" to dataProviderId)
      set("State" to INITIAL_STATE)
      set("NextExchangeDate" to recurringExchange.nextExchangeDate.toCloudDate())
      set("RecurringExchangeDetails" to recurringExchange.details)
      setJson("RecurringExchangeDetailsJson" to recurringExchange.details)
    }

    return recurringExchange.copy {
      externalRecurringExchangeId = externalId.value
      state = INITIAL_STATE
    }
  }
}
