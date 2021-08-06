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

import com.google.cloud.spanner.Value
import com.google.common.truth.Truth.assertThat
import com.google.type.Date
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.internal.kingdom.Exchange
import org.wfanet.measurement.internal.kingdom.ExchangeStep
import org.wfanet.measurement.internal.kingdom.RecurringExchange
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.KingdomDatabaseTestBase
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.ClaimReadyExchangeStep.Result

private const val DATA_PROVIDER_ID = 1L
private const val EXTERNAL_DATA_PROVIDER_ID = 2L
private const val DATA_PROVIDER_ID2 = 3L
private const val EXTERNAL_DATA_PROVIDER_ID2 = 4L
private const val MODEL_PROVIDER_ID = 5L
private const val EXTERNAL_MODEL_PROVIDER_ID = 6L
private const val MODEL_PROVIDER_ID2 = 7L
private const val EXTERNAL_MODEL_PROVIDER_ID2 = 8L
private const val RECURRING_EXCHANGE_ID1 = 9L
private const val EXTERNAL_RECURRING_EXCHANGE_ID1 = 10L
private const val RECURRING_EXCHANGE_ID2 = 11L
private const val EXTERNAL_RECURRING_EXCHANGE_ID2 = 12L
private const val RECURRING_EXCHANGE_ID3 = 13L
private const val EXTERNAL_RECURRING_EXCHANGE_ID3 = 14L
private const val DATA_PROVIDER_ID3 = 15L
private const val EXTERNAL_DATA_PROVIDER_ID3 = 16L

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
      month = 2
      day = 25
    }
    .build()

private val EXCHANGE_STEP =
  ExchangeStep.newBuilder()
    .apply {
      externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID1
      externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
      date = DATE1
      stepIndex = 1
      state = ExchangeStep.State.IN_PROGRESS
      updateTime = Value.COMMIT_TIMESTAMP.toProto()
    }
    .build()

private val EXCHANGE_STEP2 =
  ExchangeStep.newBuilder()
    .apply {
      externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID2
      externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID2
      date = DATE2
      stepIndex = 4
      state = ExchangeStep.State.IN_PROGRESS
      updateTime = Value.COMMIT_TIMESTAMP.toProto()
    }
    .build()

@RunWith(JUnit4::class)
class ClaimReadyExchangeStepTest : KingdomDatabaseTestBase() {
  @Before
  fun populateDatabase() = runBlocking {
    insertDataProvider(DATA_PROVIDER_ID, EXTERNAL_DATA_PROVIDER_ID)
    insertDataProvider(DATA_PROVIDER_ID2, EXTERNAL_DATA_PROVIDER_ID2)
    insertDataProvider(DATA_PROVIDER_ID3, EXTERNAL_DATA_PROVIDER_ID3)
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
      state = RecurringExchange.State.ACTIVE,
      nextExchangeDate = DATE2
    )
    insertRecurringExchange(
      recurringExchangeId = RECURRING_EXCHANGE_ID3,
      externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID3,
      modelProviderId = MODEL_PROVIDER_ID,
      dataProviderId = DATA_PROVIDER_ID3,
      state = RecurringExchange.State.ACTIVE,
      nextExchangeDate = DATE2
    )
    insertExchange(
      recurringExchangeId = RECURRING_EXCHANGE_ID1,
      date = DATE1,
      state = Exchange.State.ACTIVE
    )
    insertExchange(
      recurringExchangeId = RECURRING_EXCHANGE_ID2,
      date = DATE2,
      state = Exchange.State.ACTIVE
    )
    insertExchange(
      recurringExchangeId = RECURRING_EXCHANGE_ID3,
      date = DATE1,
      state = Exchange.State.ACTIVE
    )
    insertExchangeStep(
      recurringExchangeId = RECURRING_EXCHANGE_ID1,
      date = DATE1,
      stepIndex = 1L,
      state = ExchangeStep.State.READY,
      modelProviderId = MODEL_PROVIDER_ID,
      dataProviderId = null
    )
    insertExchangeStep(
      recurringExchangeId = RECURRING_EXCHANGE_ID2,
      date = DATE2,
      stepIndex = 4L,
      state = ExchangeStep.State.READY,
      modelProviderId = null,
      dataProviderId = DATA_PROVIDER_ID2
    )
    insertExchangeStep(
      recurringExchangeId = RECURRING_EXCHANGE_ID3,
      date = DATE1,
      stepIndex = 1L,
      state = ExchangeStep.State.BLOCKED,
      modelProviderId = null,
      dataProviderId = DATA_PROVIDER_ID
    )
  }

  @Test
  fun `claimReadyExchangeStep with model provider`() =
    runBlocking<Unit> {
      val expected = Result(step = EXCHANGE_STEP, attemptIndex = 1)

      val actual =
        ClaimReadyExchangeStep(
            externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID,
            externalDataProviderId = null
          )
          .execute(databaseClient)

      assertThat(actual.get()).isEqualTo(expected)
    }

  @Test
  fun `claimReadyExchangeStep with data provider`() =
    runBlocking<Unit> {
      val expected = Result(step = EXCHANGE_STEP2, attemptIndex = 1)

      val actual =
        ClaimReadyExchangeStep(
            externalModelProviderId = null,
            externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID2
          )
          .execute(databaseClient)

      assertThat(actual.get()).isEqualTo(expected)
    }

  @Test
  fun `claimReadyExchangeStep returns empty with wrong provider id`() =
    runBlocking<Unit> {
      val actual =
        ClaimReadyExchangeStep(
            externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID2,
            externalDataProviderId = null
          )
          .execute(databaseClient)

      assertThat(actual).isAbsent()
    }

  @Test
  fun `claimReadyExchangeStep returns empty without ready step`() =
    runBlocking<Unit> {
      val actual =
        ClaimReadyExchangeStep(
            externalModelProviderId = EXTERNAL_DATA_PROVIDER_ID,
            externalDataProviderId = null
          )
          .execute(databaseClient)

      assertThat(actual).isAbsent()
    }
}
