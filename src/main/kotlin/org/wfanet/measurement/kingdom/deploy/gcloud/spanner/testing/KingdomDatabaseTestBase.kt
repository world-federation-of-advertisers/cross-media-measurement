// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing

import com.google.cloud.ByteArray
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Value
import com.google.type.Date
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.gcloud.common.toCloudDate
import org.wfanet.measurement.gcloud.spanner.insertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.testing.UsingSpannerEmulator
import org.wfanet.measurement.internal.kingdom.Exchange
import org.wfanet.measurement.internal.kingdom.ExchangeDetails
import org.wfanet.measurement.internal.kingdom.ExchangeStep
import org.wfanet.measurement.internal.kingdom.RecurringExchange
import org.wfanet.measurement.internal.kingdom.RecurringExchangeDetails
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ExchangeStepReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.RecurringExchangeReader

// TODO(@yunyeng): Delete once not used.
abstract class KingdomDatabaseTestBase : UsingSpannerEmulator(KINGDOM_SCHEMA) {
  private suspend fun write(mutation: Mutation) = databaseClient.write(mutation)

  protected suspend fun insertDataProvider(dataProviderId: Long, externalDataProviderId: Long) {
    write(
      Mutation.newInsertBuilder("DataProviders")
        .set("DataProviderId")
        .to(dataProviderId)
        .set("ExternalDataProviderId")
        .to(externalDataProviderId)
        .set("DataProviderDetails")
        .to(ByteArray.copyFrom(""))
        .set("DataProviderDetailsJson")
        .to("")
        .build()
    )
  }

  protected suspend fun insertModelProvider(modelProviderId: Long, externalModelProviderId: Long) {
    write(
      insertMutation("ModelProviders") {
        set("modelProviderId" to modelProviderId)
        set("ExternalModelProviderId" to externalModelProviderId)
      }
    )
  }

  suspend fun insertRecurringExchange(
    recurringExchangeId: Long,
    externalRecurringExchangeId: Long,
    modelProviderId: Long,
    dataProviderId: Long,
    state: RecurringExchange.State,
    nextExchangeDate: Date,
    recurringExchangeDetails: RecurringExchangeDetails =
      RecurringExchangeDetails.getDefaultInstance()
  ) {
    write(
      insertMutation("RecurringExchanges") {
        set("RecurringExchangeId" to recurringExchangeId)
        set("ExternalRecurringExchangeId" to externalRecurringExchangeId)
        set("ModelProviderId" to modelProviderId)
        set("DataProviderId" to dataProviderId)
        set("State" to state)
        set("NextExchangeDate" to nextExchangeDate.toCloudDate())
        set("RecurringExchangeDetails" to recurringExchangeDetails)
        set("RecurringExchangeDetailsJson" to recurringExchangeDetails)
      }
    )
  }

  suspend fun insertExchange(
    recurringExchangeId: Long,
    date: Date,
    state: Exchange.State,
    exchangeDetails: ExchangeDetails = ExchangeDetails.getDefaultInstance()
  ) {
    write(
      insertMutation("Exchanges") {
        set("RecurringExchangeId" to recurringExchangeId)
        set("Date" to date.toCloudDate())
        set("State" to state)
        set("ExchangeDetails" to exchangeDetails)
        set("ExchangeDetailsJson" to exchangeDetails)
      }
    )
  }

  suspend fun insertExchangeStep(
    recurringExchangeId: Long,
    date: Date,
    stepIndex: Long,
    state: ExchangeStep.State,
    modelProviderId: Long? = null,
    dataProviderId: Long? = null
  ) {
    write(
      insertMutation("ExchangeSteps") {
        set("RecurringExchangeId" to recurringExchangeId)
        set("Date" to date.toCloudDate())
        set("StepIndex" to stepIndex)
        set("State" to state)
        set("UpdateTime" to Value.COMMIT_TIMESTAMP)
        set("ModelProviderId" to modelProviderId)
        set("DataProviderId" to dataProviderId)
      }
    )
  }

  protected fun readAllRecurringExchangesInSpanner(): List<RecurringExchange> = runBlocking {
    RecurringExchangeReader()
      .execute(databaseClient.singleUse())
      .map { it.recurringExchange }
      .toList()
  }

  protected fun readAllExchangeStepsInSpanner(): List<ExchangeStep> = runBlocking {
    ExchangeStepReader().execute(databaseClient.singleUse()).map { it.exchangeStep }.toList()
  }

  // TODO: add helpers for other tables.
}
