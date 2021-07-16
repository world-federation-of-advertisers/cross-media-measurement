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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.type.Date
import java.time.Instant
import kotlin.test.assertFails
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.internal.kingdom.Exchange
import org.wfanet.measurement.internal.kingdom.ExchangeDetails
import org.wfanet.measurement.internal.kingdom.ExchangeStep
import org.wfanet.measurement.internal.kingdom.RecurringExchange
import org.wfanet.measurement.kingdom.db.getExchangeStepFilter
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
      month = 3
      day = 1
    }
    .build()
private val DATE3 =
  Date.newBuilder()
    .apply {
      year = 2021
      month = 6
      day = 28
    }
    .build()
private val EXCHANGE_DETAILS =
  ExchangeDetails.newBuilder().setAuditTrailHash(ByteString.copyFromUtf8("123")).build()

@RunWith(JUnit4::class)
class GetExchangeStepTest : KingdomDatabaseTestBase() {
  @Before
  fun populateDatabase() = runBlocking {
    insertDataProvider(DATA_PROVIDER_ID, EXTERNAL_DATA_PROVIDER_ID)
    insertDataProvider(DATA_PROVIDER_ID2, EXTERNAL_DATA_PROVIDER_ID2)
    insertModelProvider(MODEL_PROVIDER_ID, EXTERNAL_MODEL_PROVIDER_ID)

    insertRecurringExchange(
      RECURRING_EXCHANGE_ID1,
      EXTERNAL_RECURRING_EXCHANGE_ID1,
      MODEL_PROVIDER_ID,
      DATA_PROVIDER_ID,
      RecurringExchange.State.ACTIVE,
      DATE1
    )
    insertRecurringExchange(
      RECURRING_EXCHANGE_ID2,
      EXTERNAL_RECURRING_EXCHANGE_ID2,
      MODEL_PROVIDER_ID,
      DATA_PROVIDER_ID2,
      RecurringExchange.State.RETIRED,
      DATE2
    )
    insertExchange(RECURRING_EXCHANGE_ID1, DATE1, Exchange.State.ACTIVE, EXCHANGE_DETAILS)
    insertExchange(RECURRING_EXCHANGE_ID2, DATE2, Exchange.State.FAILED, EXCHANGE_DETAILS)
    insertExchange(RECURRING_EXCHANGE_ID1, DATE3, Exchange.State.ACTIVE, EXCHANGE_DETAILS)

    insertExchangeStep(
      RECURRING_EXCHANGE_ID1,
      DATE1,
      1L,
      ExchangeStep.State.READY,
      Instant.now().minusSeconds(1000),
      null,
      DATA_PROVIDER_ID
    )
    insertExchangeStep(
      RECURRING_EXCHANGE_ID2,
      DATE2,
      1L,
      ExchangeStep.State.BLOCKED,
      Instant.now(),
      null,
      DATA_PROVIDER_ID
    )
    insertExchangeStep(
      RECURRING_EXCHANGE_ID1,
      DATE3,
      2L,
      ExchangeStep.State.READY_FOR_RETRY,
      Instant.now(),
      MODEL_PROVIDER_ID,
      null
    )
  }

  @Test
  fun `the Exchange Step with Data Provider exists`() = runBlocking {
    val filter =
      getExchangeStepFilter(externalDataProviderId = ExternalId(EXTERNAL_DATA_PROVIDER_ID))
    val exchangeStep =
      GetExchangeStep(filter).executeSingle(databaseClient.singleUse()).exchangeStep

    val expected =
      ExchangeStep.newBuilder()
        .apply {
          externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID1
          date = DATE1
          stepIndex = 1
          state = ExchangeStep.State.READY
          externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
        }
        .build()

    assertThat(exchangeStep).comparingExpectedFieldsOnly().isEqualTo(expected)
  }

  @Test
  fun `the Exchange Step with Model Provider exists`() = runBlocking {
    val filter =
      getExchangeStepFilter(externalModelProviderId = ExternalId(EXTERNAL_MODEL_PROVIDER_ID))
    val exchangeStep =
      GetExchangeStep(filter).executeSingle(databaseClient.singleUse()).exchangeStep

    val expected =
      ExchangeStep.newBuilder()
        .apply {
          externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID1
          date = DATE3
          stepIndex = 2
          state = ExchangeStep.State.READY_FOR_RETRY
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
        }
        .build()

    assertThat(exchangeStep).comparingExpectedFieldsOnly().isEqualTo(expected)
  }

  @Test
  fun `the Exchange Step with Primary Keys exists`() = runBlocking {
    val filter =
      getExchangeStepFilter(
        recurringExchangeId = RECURRING_EXCHANGE_ID1,
        date = DATE1,
        stepIndex = 1L
      )
    val exchangeStep =
      GetExchangeStep(filter).executeSingle(databaseClient.singleUse()).exchangeStep

    val expected =
      ExchangeStep.newBuilder()
        .apply {
          externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID1
          date = DATE1
          stepIndex = 1
          state = ExchangeStep.State.READY
          externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
        }
        .build()

    assertThat(exchangeStep).comparingExpectedFieldsOnly().isEqualTo(expected)
  }

  @Test
  fun `the Exchange Step does not exist`() =
    runBlocking<Unit> {
      val filter =
        getExchangeStepFilter(
          externalDataProviderId = ExternalId(EXTERNAL_DATA_PROVIDER_ID),
          states = listOf(ExchangeStep.State.READY_FOR_RETRY)
        )

      assertFails { GetExchangeStep(filter).executeSingle(databaseClient.singleUse()) }
    }
}
