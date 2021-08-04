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
import org.wfanet.measurement.internal.kingdom.ExchangeWorkflow
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
      stepIndex = 4
      state = ExchangeStep.State.IN_PROGRESS
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

private val EXCHANGE_WORKFLOW1 =
  ExchangeWorkflow.newBuilder()
    .apply {
      addAllSteps(
        mutableListOf(
          ExchangeWorkflow.Step.newBuilder()
            .apply {
              party = ExchangeWorkflow.Party.MODEL_PROVIDER
              stepIndex = 1
              addAllPrerequisiteStepIndices(emptyList())
            }
            .build(),
          ExchangeWorkflow.Step.newBuilder()
            .apply {
              party = ExchangeWorkflow.Party.MODEL_PROVIDER
              stepIndex = 2
              addAllPrerequisiteStepIndices(listOf(1))
            }
            .build(),
          ExchangeWorkflow.Step.newBuilder()
            .apply {
              party = ExchangeWorkflow.Party.DATA_PROVIDER
              stepIndex = 3
              addAllPrerequisiteStepIndices(listOf(1, 2))
            }
            .build()
        )
      )
    }
    .build()

private val EXCHANGE_WORKFLOW2 =
  ExchangeWorkflow.newBuilder()
    .apply {
      addAllSteps(
        mutableListOf(
          ExchangeWorkflow.Step.newBuilder()
            .apply {
              party = ExchangeWorkflow.Party.DATA_PROVIDER
              stepIndex = 1
              addAllPrerequisiteStepIndices(listOf(2))
            }
            .build(),
          ExchangeWorkflow.Step.newBuilder()
            .apply {
              party = ExchangeWorkflow.Party.MODEL_PROVIDER
              stepIndex = 2
              addAllPrerequisiteStepIndices(listOf(3, 4))
            }
            .build(),
          ExchangeWorkflow.Step.newBuilder()
            .apply {
              party = ExchangeWorkflow.Party.DATA_PROVIDER
              stepIndex = 3
              addAllPrerequisiteStepIndices(listOf(4))
            }
            .build(),
          ExchangeWorkflow.Step.newBuilder()
            .apply {
              party = ExchangeWorkflow.Party.DATA_PROVIDER
              stepIndex = 4
              addAllPrerequisiteStepIndices(listOf())
            }
            .build(),
          ExchangeWorkflow.Step.newBuilder()
            .apply {
              party = ExchangeWorkflow.Party.MODEL_PROVIDER
              stepIndex = 5
              addAllPrerequisiteStepIndices(listOf(1, 2, 3, 4))
            }
            .build()
        )
      )
    }
    .build()

private val EXCHANGE_WORKFLOW3 =
  ExchangeWorkflow.newBuilder()
    .apply {
      addAllSteps(
        mutableListOf(
          ExchangeWorkflow.Step.newBuilder()
            .apply {
              party = ExchangeWorkflow.Party.DATA_PROVIDER
              stepIndex = 1
              addAllPrerequisiteStepIndices(listOf(2))
            }
            .build(),
          ExchangeWorkflow.Step.newBuilder()
            .apply {
              party = ExchangeWorkflow.Party.DATA_PROVIDER
              stepIndex = 2
              addAllPrerequisiteStepIndices(listOf(1))
            }
            .build()
        )
      )
    }
    .build()

private val RECURRING_EXCHANGE_DETAILS =
  RecurringExchangeDetails.newBuilder()
    .setCronSchedule("DAILY")
    .setExchangeWorkflow(EXCHANGE_WORKFLOW1)
    .build()
private val RECURRING_EXCHANGE_DETAILS2 =
  RecurringExchangeDetails.newBuilder()
    .setCronSchedule("WEEKLY")
    .setExchangeWorkflow(EXCHANGE_WORKFLOW2)
    .build()
private val RECURRING_EXCHANGE_DETAILS3 =
  RecurringExchangeDetails.newBuilder()
    .setCronSchedule("MONTHLY")
    .setExchangeWorkflow(EXCHANGE_WORKFLOW3)
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
    insertRecurringExchange(
      recurringExchangeId = RECURRING_EXCHANGE_ID3,
      externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID3,
      modelProviderId = MODEL_PROVIDER_ID,
      dataProviderId = DATA_PROVIDER_ID3,
      state = RecurringExchange.State.ACTIVE,
      nextExchangeDate = DATE2,
      recurringExchangeDetails = RECURRING_EXCHANGE_DETAILS3
    )
  }

  @Test
  fun `claimReadyExchangeStep with model provider`() =
    runBlocking<Unit> {
      val expected: ClaimReadyExchangeStep.Result =
        ClaimReadyExchangeStep.Result(
          step = EXCHANGE_STEP,
          attemptNumber = EXCHANGE_STEP_ATTEMPT.attemptNumber
        )

      val actual =
        ClaimReadyExchangeStep(
            externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID,
            externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          )
          .execute(databaseClient)

      assertThat(actual.get()).isEqualTo(expected)

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
  fun `claimReadyExchangeStep with data provider`() =
    runBlocking<Unit> {
      val expected: ClaimReadyExchangeStep.Result =
        ClaimReadyExchangeStep.Result(
          step = EXCHANGE_STEP2,
          attemptNumber = EXCHANGE_STEP_ATTEMPT2.attemptNumber
        )

      val actual =
        ClaimReadyExchangeStep(
            externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID,
            externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID2
          )
          .execute(databaseClient)

      assertThat(actual.get()).isEqualTo(expected)

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
  fun `claimReadyExchangeStep with wrong provider id`() =
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
  fun `claimReadyExchangeStep with existing step`() =
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

      assertThat(actual.get().step).isEqualTo(expected)
      // Assert that ExchangeStep.Status updated to IN_PROGRESS.
      assertThat(readAllExchangeStepsInSpanner().first().state)
        .isEqualTo(ExchangeStep.State.IN_PROGRESS)
    }

  @Test
  fun `claimReadyExchangeStep with invalid workflow`() =
    runBlocking<Unit> {
      val exception =
        assertFailsWith<IllegalStateException> {
          ClaimReadyExchangeStep(
              externalModelProviderId = null,
              externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID3
            )
            .execute(databaseClient)
        }
      assertThat(exception).hasMessageThat().contains("No Exchange Steps were found.")
    }
}
