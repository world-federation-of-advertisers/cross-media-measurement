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
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.type.Date
import java.lang.IllegalStateException
import java.time.Instant
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.internal.kingdom.Exchange
import org.wfanet.measurement.internal.kingdom.ExchangeDetails
import org.wfanet.measurement.internal.kingdom.ExchangeStep
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttempt
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptDetails
import org.wfanet.measurement.internal.kingdom.RecurringExchange
import org.wfanet.measurement.internal.kingdom.RecurringExchangeDetails
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.RecurringExchangeReader
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
      updateTime = Value.COMMIT_TIMESTAMP.toProto()
    }
    .build()

private val EXCHANGE_STEP_ATTEMPT =
  ExchangeStepAttempt.newBuilder()
    .apply {
      externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID1
      date = DATE1
      stepIndex = 1
      state = ExchangeStepAttempt.State.ACTIVE
      attemptNumber = 1
      details = ExchangeStepAttemptDetails.getDefaultInstance()
    }
    .build()

private val EXCHANGE_STEP2 =
  ExchangeStep.newBuilder()
    .apply {
      externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID2
      externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID2
      date = DATE2
      stepIndex = 1
      state = ExchangeStep.State.IN_PROGRESS
      updateTime = Value.COMMIT_TIMESTAMP.toProto()
    }
    .build()

private val EXCHANGE_STEP_ATTEMPT2 =
  ExchangeStepAttempt.newBuilder()
    .apply {
      externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID2
      date = DATE2
      stepIndex = 1
      state = ExchangeStepAttempt.State.ACTIVE
      attemptNumber = 1
      details = ExchangeStepAttemptDetails.getDefaultInstance()
    }
    .build()

@RunWith(JUnit4::class)
class ClaimReadyExchangeStepTest : KingdomDatabaseTestBase() {
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
      val expected: ClaimReadyExchangeStep.Result =
        ClaimReadyExchangeStep.Result(step = EXCHANGE_STEP, attempt = EXCHANGE_STEP_ATTEMPT)

      val actual =
        ClaimReadyExchangeStep(
            externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID,
            externalDataProviderId = null
          )
          .execute(databaseClient)

      assertThat(actual).isEqualTo(expected)

      // Assert that RecurringExchange.nextExchangeDate updated.
      assertThat(
          RecurringExchangeReader()
            .readExternalId(databaseClient.singleUse(), ExternalId(EXTERNAL_RECURRING_EXCHANGE_ID1))
            .recurringExchange
            .nextExchangeDate
        )
        .isEqualTo(DATE1_UPDATED)
    }

  @Test
  fun claimReadyExchangeStepWithDataProvider() =
    runBlocking<Unit> {
      val expected: ClaimReadyExchangeStep.Result =
        ClaimReadyExchangeStep.Result(step = EXCHANGE_STEP2, attempt = EXCHANGE_STEP_ATTEMPT2)

      val actual =
        ClaimReadyExchangeStep(
            externalModelProviderId = null,
            externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID2
          )
          .execute(databaseClient)

      assertThat(actual).isEqualTo(expected)

      // Assert that RecurringExchange.nextExchangeDate updated.
      assertThat(
          RecurringExchangeReader()
            .readExternalId(databaseClient.singleUse(), ExternalId(EXTERNAL_RECURRING_EXCHANGE_ID2))
            .recurringExchange
            .nextExchangeDate
        )
        .isEqualTo(DATE2_UPDATED)
    }

  @Test
  fun claimReadyExchangeStepWithWrongProviderId() =
    runBlocking<Unit> {
      val exception =
        assertFailsWith<IllegalStateException> {
          ClaimReadyExchangeStep(
              externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID2,
              externalDataProviderId = null
            )
            .execute(databaseClient)
        }
      assertThat(exception.localizedMessage).isEqualTo("No Exchange Steps were found.")
    }

  @Test
  fun claimReadyExchangeStepWithExistingStep() =
    runBlocking<Unit> {
      insertExchange(
        recurringExchangeId = RECURRING_EXCHANGE_ID1,
        date = DATE1,
        state = Exchange.State.ACTIVE,
        exchangeDetails = ExchangeDetails.getDefaultInstance()
      )
      insertExchangeStep(
        recurringExchangeId = RECURRING_EXCHANGE_ID1,
        date = DATE1,
        stepIndex = 1L,
        state = ExchangeStep.State.READY,
        updateTime = Instant.now().minusSeconds(1000),
        modelProviderId = MODEL_PROVIDER_ID,
        dataProviderId = null
      )
      val expected =
        ExchangeStep.newBuilder()
          .apply {
            this.externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID1
            this.externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
            this.date = DATE1
            this.state = ExchangeStep.State.IN_PROGRESS
            this.stepIndex = 1
            this.updateTime = Value.COMMIT_TIMESTAMP.toProto()
          }
          .build()

      val actual =
        ClaimReadyExchangeStep(
            externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID,
            externalDataProviderId = null
          )
          .execute(databaseClient)

      assertThat(actual.step).isEqualTo(expected)
      // Assert that ExchangeStep.Status updated to IN_PROGRESS.
      assertThat(readAllExchangeStepsInSpanner().first().state)
        .isEqualTo(ExchangeStep.State.IN_PROGRESS)
    }
}
