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

package org.wfanet.measurement.kingdom.service.internal

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.type.Date
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.internal.kingdom.ClaimReadyExchangeStepRequest
import org.wfanet.measurement.internal.kingdom.ClaimReadyExchangeStepResponse
import org.wfanet.measurement.internal.kingdom.ExchangeStep
import org.wfanet.measurement.internal.kingdom.RecurringExchange
import org.wfanet.measurement.internal.kingdom.RecurringExchangeDetails
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ExchangeReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.KingdomDatabaseTestBase

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
private val DATE1_UPDATED =
  Date.newBuilder()
    .apply {
      year = 2021
      month = 1
      day = 16
    }
    .build()
private val DATE2_UPDATED =
  Date.newBuilder()
    .apply {
      year = 2021
      month = 3
      day = 4
    }
    .build()
private val RECURRING_EXCHANGE_DETAILS =
  RecurringExchangeDetails.newBuilder().setCronSchedule("DAILY").build()
private val RECURRING_EXCHANGE_DETAILS2 =
  RecurringExchangeDetails.newBuilder().setCronSchedule("WEEKLY").build()

private val EXCHANGE_STEP =
  ExchangeStep.newBuilder()
    .apply {
      externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID1
      externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
      date = DATE1
      stepIndex = 1
      state = ExchangeStep.State.IN_PROGRESS
    }
    .build()

private val EXCHANGE_STEP2 =
  ExchangeStep.newBuilder()
    .apply {
      externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID2
      externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
      date = DATE2
      stepIndex = 1
      state = ExchangeStep.State.IN_PROGRESS
    }
    .build()

@RunWith(JUnit4::class)
class ExchangeStepsServiceTest : KingdomDatabaseTestBase() {
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
      nextExchangeDate = DATE1,
      recurringExchangeDetails = RECURRING_EXCHANGE_DETAILS
    )
    insertRecurringExchange(
      recurringExchangeId = RECURRING_EXCHANGE_ID2,
      externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID2,
      modelProviderId = MODEL_PROVIDER_ID,
      dataProviderId = DATA_PROVIDER_ID2,
      state = RecurringExchange.State.ACTIVE,
      nextExchangeDate = DATE2,
      recurringExchangeDetails = RECURRING_EXCHANGE_DETAILS2
    )
  }

  @Test
  fun claimReadyExchangeStepWithModelProvider() =
    runBlocking<Unit> {
      val request =
        ClaimReadyExchangeStepRequest.newBuilder()
          .setExternalModelProviderId(EXTERNAL_MODEL_PROVIDER_ID)
          .build()

      val response: ClaimReadyExchangeStepResponse =
        ClaimReadyExchangeStepResponse.newBuilder()
          .apply {
            exchangeStep = EXCHANGE_STEP
            attemptNumber = 1
          }
          .build()

      val service = ExchangeStepsService(databaseClient)
      assertThat(service.claimReadyExchangeStep(request))
        .comparingExpectedFieldsOnly()
        .isEqualTo(response)

      // Assert that RecurringExchange.nextExchangeDate updated.
      assertThat(
          ExchangeReader()
            .readExternalId(databaseClient.singleUse(), ExternalId(EXTERNAL_RECURRING_EXCHANGE_ID1))
            .recurringExchange
            .nextExchangeDate
        )
        .isEqualTo(DATE1_UPDATED)
    }

  @Test
  fun claimReadyExchangeStepWithDataProvider() =
    runBlocking<Unit> {
      val request =
        ClaimReadyExchangeStepRequest.newBuilder()
          .setExternalDataProviderId(EXTERNAL_DATA_PROVIDER_ID2)
          .build()

      val response: ClaimReadyExchangeStepResponse =
        ClaimReadyExchangeStepResponse.newBuilder()
          .apply {
            exchangeStep = EXCHANGE_STEP2
            attemptNumber = 1
          }
          .build()

      val service = ExchangeStepsService(databaseClient)
      assertThat(service.claimReadyExchangeStep(request))
        .comparingExpectedFieldsOnly()
        .isEqualTo(response)

      // Assert that RecurringExchange.nextExchangeDate updated.
      assertThat(
          ExchangeReader()
            .readExternalId(databaseClient.singleUse(), ExternalId(EXTERNAL_RECURRING_EXCHANGE_ID2))
            .recurringExchange
            .nextExchangeDate
        )
        .isEqualTo(DATE2_UPDATED)
    }

  @Test
  fun claimReadyExchangeStepWithWrongProviderId() =
    runBlocking<Unit> {
      val request =
        ClaimReadyExchangeStepRequest.newBuilder()
          .setExternalDataProviderId(EXTERNAL_MODEL_PROVIDER_ID2)
          .build()

      val service = ExchangeStepsService(databaseClient)

      assertFailsWith<NoSuchElementException> { service.claimReadyExchangeStep(request) }
    }
}
