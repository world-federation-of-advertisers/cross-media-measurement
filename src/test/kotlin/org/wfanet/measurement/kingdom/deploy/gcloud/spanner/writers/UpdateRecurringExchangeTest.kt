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

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.type.Date
import kotlin.test.assertFails
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.gcloud.common.toCloudDate
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.setJson
import org.wfanet.measurement.gcloud.spanner.updateMutation
import org.wfanet.measurement.internal.kingdom.Exchange
import org.wfanet.measurement.internal.kingdom.ExchangeDetails
import org.wfanet.measurement.internal.kingdom.RecurringExchange
import org.wfanet.measurement.internal.kingdom.RecurringExchangeDetails
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.KingdomDatabaseTestBase

private const val DATA_PROVIDER_ID = 1L
private const val EXTERNAL_DATA_PROVIDER_ID = 2L
private const val DATA_PROVIDER_ID2 = 3L
private const val EXTERNAL_DATA_PROVIDER_ID2 = 4L
private const val MODEL_PROVIDER_ID = 5L
private const val EXTERNAL_MODEL_PROVIDER_ID = 6L
private const val RECURRING_EXCHANGE_ID1 = 7L
private const val EXTERNAL_RECURRING_EXCHANGE_ID1 = 8L
private const val RECURRING_EXCHANGE_ID2 = 9L
private const val EXTERNAL_RECURRING_EXCHANGE_ID2 = 10L

private val DATE1 =
  Date.newBuilder()
    .apply {
      year = 2021
      month = 1
      day = 15
    }
    .build()
private val DATE2 =
  Date.newBuilder()
    .apply {
      year = 2021
      month = 1
      day = 16
    }
    .build()
private val DATE3 =
  Date.newBuilder()
    .apply {
      year = 2021
      month = 1
      day = 22
    }
    .build()
private val DATE4 =
  Date.newBuilder()
    .apply {
      year = 2021
      month = 2
      day = 15
    }
    .build()
private val EXCHANGE_DETAILS =
  ExchangeDetails.newBuilder().setAuditTrailHash(ByteString.copyFromUtf8("123")).build()
private val RECURRING_EXCHANGE_DETAILS =
  RecurringExchangeDetails.newBuilder().setCronSchedule("DAILY").build()
private val RECURRING_EXCHANGE_DETAILS2 =
  RecurringExchangeDetails.newBuilder().setCronSchedule("WEEKLY").build()
private val RECURRING_EXCHANGE_DETAILS3 =
  RecurringExchangeDetails.newBuilder().setCronSchedule("MONTHLY").build()

private val RECURRING_EXCHANGE =
  RecurringExchange.newBuilder()
    .apply {
      externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID1
      externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
      nextExchangeDate = DATE1
      state = RecurringExchange.State.ACTIVE
      details = RECURRING_EXCHANGE_DETAILS
    }
    .build()

private val RECURRING_EXCHANGE2 =
  RecurringExchange.newBuilder()
    .apply {
      externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID2
      externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
      nextExchangeDate = DATE1
      state = RecurringExchange.State.ACTIVE
      details = RECURRING_EXCHANGE_DETAILS2
    }
    .build()

@RunWith(JUnit4::class)
class UpdateRecurringExchangeTest : KingdomDatabaseTestBase() {
  @Before
  fun populateDatabase() = runBlocking {
    insertDataProvider(DATA_PROVIDER_ID, EXTERNAL_DATA_PROVIDER_ID)
    insertDataProvider(DATA_PROVIDER_ID2, EXTERNAL_DATA_PROVIDER_ID2)
    insertModelProvider(MODEL_PROVIDER_ID, EXTERNAL_MODEL_PROVIDER_ID)

    insertRecurringExchange(
      recurringExchangeId = RECURRING_EXCHANGE_ID1,
      externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID1,
      modelProviderId = MODEL_PROVIDER_ID,
      dataProviderId = DATA_PROVIDER_ID,
      state = RecurringExchange.State.ACTIVE,
      nextExchangeDate = DATE1
    )
    insertRecurringExchange(
      recurringExchangeId = RECURRING_EXCHANGE_ID2,
      externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID2,
      modelProviderId = MODEL_PROVIDER_ID,
      dataProviderId = DATA_PROVIDER_ID2,
      state = RecurringExchange.State.RETIRED,
      nextExchangeDate = DATE2
    )
    insertExchange(
      recurringExchangeId = RECURRING_EXCHANGE_ID1,
      date = DATE1,
      state = Exchange.State.ACTIVE,
      exchangeDetails = EXCHANGE_DETAILS
    )
    insertExchange(
      recurringExchangeId = RECURRING_EXCHANGE_ID2,
      date = DATE2,
      state = Exchange.State.FAILED,
      exchangeDetails = EXCHANGE_DETAILS
    )
    insertExchange(
      recurringExchangeId = RECURRING_EXCHANGE_ID1,
      date = DATE3,
      state = Exchange.State.ACTIVE,
      exchangeDetails = EXCHANGE_DETAILS
    )
  }

  private suspend fun directlyUpdate(nextExchangeDate: Date, state: RecurringExchange.State) {
    databaseClient.write(
      updateMutation("RecurringExchanges") {
        set("RecurringExchangeId" to RECURRING_EXCHANGE_ID1)
        set("ExternalRecurringExchangeId" to EXTERNAL_RECURRING_EXCHANGE_ID1)
        set("ModelProviderId" to MODEL_PROVIDER_ID)
        set("State" to state)
        set("NextExchangeDate" to nextExchangeDate.toCloudDate())
        set("RecurringExchangeDetails" to RECURRING_EXCHANGE_DETAILS)
        setJson("RecurringExchangeDetailsJson" to RECURRING_EXCHANGE_DETAILS)
      }
    )
  }

  private suspend fun updateAndAssertSuccess(
    nextExchangeDate: Date,
    state: RecurringExchange.State
  ) {
    val recurringExchange =
      UpdateRecurringExchange(RECURRING_EXCHANGE, RECURRING_EXCHANGE_ID1, nextExchangeDate, state)
        .execute(databaseClient)

    assertThat(recurringExchange)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        RecurringExchange.newBuilder()
          .apply {
            externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID1
            externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
            this.nextExchangeDate = nextExchangeDate
            this.state = state
            this.details = RECURRING_EXCHANGE_DETAILS
          }
          .build()
      )
  }

  @Test
  fun `state update in normal flow`() = runBlocking {
    directlyUpdate(DATE2, RecurringExchange.State.ACTIVE)
    updateAndAssertSuccess(DATE3, RecurringExchange.State.ACTIVE)
    updateAndAssertSuccess(DATE1, RecurringExchange.State.RETIRED)
  }

  @Test
  fun `terminal states do not allow updates`() =
    runBlocking<Unit> {
      val recurringExchange =
        RecurringExchange.newBuilder()
          .apply {
            externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID1
            externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
            nextExchangeDate = DATE1
            state = RecurringExchange.State.RETIRED
            details = RECURRING_EXCHANGE_DETAILS
          }
          .build()
      assertFails {
        UpdateRecurringExchange(
            recurringExchange,
            RECURRING_EXCHANGE_ID1,
            DATE2,
            RecurringExchange.State.ACTIVE
          )
          .execute(databaseClient)
      }
    }

  @Test
  fun `noop update`() = runBlocking {
    // Does not fail:
    updateAndAssertSuccess(DATE1, RecurringExchange.State.ACTIVE)
  }
}
