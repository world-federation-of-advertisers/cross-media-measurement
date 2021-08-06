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
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttempt
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptDetails
import org.wfanet.measurement.internal.kingdom.ExchangeWorkflow
import org.wfanet.measurement.internal.kingdom.FinishExchangeStepAttemptRequest
import org.wfanet.measurement.internal.kingdom.RecurringExchange
import org.wfanet.measurement.internal.kingdom.RecurringExchangeDetails
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

private val RECURRING_EXCHANGE_DETAILS =
  RecurringExchangeDetails.newBuilder()
    .setExchangeWorkflow(
      ExchangeWorkflow.newBuilder().apply {
        addSteps(
          ExchangeWorkflow.Step.newBuilder().apply {
            party = ExchangeWorkflow.Party.DATA_PROVIDER
            stepIndex = 1
          }
        )
      }
    )
    .build()

private val EXCHANGE_STEP =
  ExchangeStep.newBuilder()
    .apply {
      externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID1
      externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
      date = DATE1
      stepIndex = 1
      state = ExchangeStep.State.IN_PROGRESS
    }
    .build()

private val EXCHANGE_STEP_ATTEMPT =
  ExchangeStepAttempt.newBuilder()
    .apply {
      externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID1
      date = DATE1
      stepIndex = 1
      attemptNumber = 1
      state = ExchangeStepAttempt.State.ACTIVE
    }
    .build()

@RunWith(JUnit4::class)
class FinishExchangeStepAttemptTest : KingdomDatabaseTestBase() {
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
      nextExchangeDate = DATE1,
      recurringExchangeDetails = RECURRING_EXCHANGE_DETAILS
    )
    insertExchange(
      recurringExchangeId = RECURRING_EXCHANGE_ID1,
      date = DATE1,
      state = Exchange.State.ACTIVE
    )
    insertExchangeStep(
      recurringExchangeId = RECURRING_EXCHANGE_ID1,
      date = DATE1,
      stepIndex = 1L,
      state = ExchangeStep.State.IN_PROGRESS,
      modelProviderId = null,
      dataProviderId = DATA_PROVIDER_ID
    )
    insertExchangeStepAttempt(
      recurringExchangeId = RECURRING_EXCHANGE_ID1,
      date = DATE1,
      stepIndex = 1L,
      attemptIndex = 1L,
      state = ExchangeStepAttempt.State.ACTIVE
    )
  }

  @Test
  fun `finishExchangeStepAttempt succeeds`() =
    runBlocking<Unit> {
      val request =
        FinishExchangeStepAttemptRequest.newBuilder()
          .apply {
            externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID1
            date = DATE1
            stepIndex = EXCHANGE_STEP.stepIndex
            attemptNumber = EXCHANGE_STEP_ATTEMPT.attemptNumber
            state = ExchangeStepAttempt.State.SUCCEEDED
          }
          .build()

      val debugLog =
        ExchangeStepAttemptDetails.DebugLog.newBuilder()
          .apply {
            message = "Attempt for Step: ${EXCHANGE_STEP.stepIndex} Succeeded."
            time = Value.COMMIT_TIMESTAMP.toProto()
          }
          .build()

      val logDetails =
        ExchangeStepAttemptDetails.newBuilder()
          .apply {
            updateTime = Value.COMMIT_TIMESTAMP.toProto()
            addDebugLogEntries(debugLog)
          }
          .build()

      val expected =
        EXCHANGE_STEP_ATTEMPT
          .toBuilder()
          .apply {
            state = ExchangeStepAttempt.State.SUCCEEDED
            details = logDetails
          }
          .build()

      val actual = FinishExchangeStepAttempt(request).execute(databaseClient)

      assertThat(actual).isEqualTo(expected)
      assertThat(readAllExchangeStepsInSpanner())
        .containsExactly(EXCHANGE_STEP.toBuilder().setState(ExchangeStep.State.SUCCEEDED).build())
    }

  @Test
  fun `finishExchangeStepAttempt temporarilyFails`() =
    runBlocking<Unit> {
      val request =
        FinishExchangeStepAttemptRequest.newBuilder()
          .apply {
            externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID1
            date = DATE1
            stepIndex = EXCHANGE_STEP.stepIndex
            attemptNumber = EXCHANGE_STEP_ATTEMPT.attemptNumber
            state = ExchangeStepAttempt.State.FAILED
          }
          .build()

      val debugLog =
        ExchangeStepAttemptDetails.DebugLog.newBuilder()
          .apply {
            message = "Attempt for Step: ${EXCHANGE_STEP.stepIndex} Failed."
            time = Value.COMMIT_TIMESTAMP.toProto()
          }
          .build()

      val logDetails =
        ExchangeStepAttemptDetails.newBuilder()
          .apply {
            updateTime = Value.COMMIT_TIMESTAMP.toProto()
            addDebugLogEntries(debugLog)
          }
          .build()

      val expected =
        EXCHANGE_STEP_ATTEMPT
          .toBuilder()
          .apply {
            state = ExchangeStepAttempt.State.FAILED
            details = logDetails
          }
          .build()
      val actual = FinishExchangeStepAttempt(request).execute(databaseClient)

      assertThat(actual).isEqualTo(expected)
      assertThat(readAllExchangeStepsInSpanner())
        .containsExactly(
          EXCHANGE_STEP.toBuilder().setState(ExchangeStep.State.READY_FOR_RETRY).build()
        )
    }

    @Test
    fun `finishExchangeStepAttempt permanentlyFails`() =
      runBlocking<Unit> {
        val request =
          FinishExchangeStepAttemptRequest.newBuilder()
            .apply {
              externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID1
              date = DATE1
              stepIndex = EXCHANGE_STEP.stepIndex
              attemptNumber = EXCHANGE_STEP_ATTEMPT.attemptNumber
              state = ExchangeStepAttempt.State.FAILED_STEP
            }
            .build()

        val debugLog =
          ExchangeStepAttemptDetails.DebugLog.newBuilder()
            .apply {
              message = "Attempt for Step: ${EXCHANGE_STEP.stepIndex} Failed."
              time = Value.COMMIT_TIMESTAMP.toProto()
            }
            .build()

        val logDetails =
          ExchangeStepAttemptDetails.newBuilder()
            .apply {
              updateTime = Value.COMMIT_TIMESTAMP.toProto()
              addDebugLogEntries(debugLog)
            }
            .build()

        val expected =
          EXCHANGE_STEP_ATTEMPT
            .toBuilder()
            .apply {
              state = ExchangeStepAttempt.State.FAILED_STEP
              details = logDetails
            }
            .build()
        val actual = FinishExchangeStepAttempt(request).execute(databaseClient)

        assertThat(actual).isEqualTo(expected)
        assertThat(readAllExchangeStepsInSpanner())
          .containsExactly(
            EXCHANGE_STEP.toBuilder().setState(ExchangeStep.State.FAILED).build()
          )
      }
}
