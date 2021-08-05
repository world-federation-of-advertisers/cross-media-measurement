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

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.type.Date
import java.lang.IllegalArgumentException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.internal.kingdom.ExchangeStep
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
private val DATE3 =
  Date.newBuilder()
    .apply {
      year = 2021
      month = 5
      day = 5
    }
    .build()
private val DATE3_UPDATED =
  Date.newBuilder()
    .apply {
      year = 2021
      month = 6
      day = 5
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
              party = ExchangeWorkflow.Party.MODEL_PROVIDER
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
class CreateExchangesAndStepsTest : KingdomDatabaseTestBase() {
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
      nextExchangeDate = DATE3,
      recurringExchangeDetails = RECURRING_EXCHANGE_DETAILS3
    )
  }

  @Test
  fun `createExchangesAndSteps with model provider`() =
    runBlocking<Unit> {
      CreateExchangesAndSteps(
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID,
          externalDataProviderId = null
        )
        .execute(databaseClient)

      // Assert that RecurringExchange.nextExchangeDate updated.
      assertThat(
          RecurringExchangeReader()
            .readExternalId(databaseClient.singleUse(), ExternalId(EXTERNAL_RECURRING_EXCHANGE_ID1))
            .recurringExchange
            .nextExchangeDate
        )
        .isEqualTo(DATE1_UPDATED)
      // Assert that all the ExchangeSteps are created.
      assertThat(readAllExchangeStepsInSpanner()).hasSize(EXCHANGE_WORKFLOW1.stepsCount)
    }

  @Test
  fun `createExchangesAndSteps with data provider`() =
    runBlocking<Unit> {
      CreateExchangesAndSteps(
          externalModelProviderId = null,
          externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID2
        )
        .execute(databaseClient)

      // Assert that RecurringExchange.nextExchangeDate updated.
      assertThat(
          RecurringExchangeReader()
            .readExternalId(databaseClient.singleUse(), ExternalId(EXTERNAL_RECURRING_EXCHANGE_ID2))
            .recurringExchange
            .nextExchangeDate
        )
        .isEqualTo(DATE2_UPDATED)
      // Assert that all the ExchangeSteps are created.
      assertThat(readAllExchangeStepsInSpanner()).hasSize(EXCHANGE_WORKFLOW2.stepsCount)
    }

  @Test
  fun `createExchangesAndSteps with wrong provider id`() =
    runBlocking<Unit> {
          CreateExchangesAndSteps(
              externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID2,
              externalDataProviderId = null
            )
            .execute(databaseClient)

      assertThat(readAllExchangeStepsInSpanner()).hasSize(0)
    }

  @Test
  fun `createExchangesAndSteps with invalid workflow`() =
    runBlocking<Unit> {
      CreateExchangesAndSteps(
          externalModelProviderId = null,
          externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID3
        )
        .execute(databaseClient)

      // Assert that RecurringExchange.nextExchangeDate updated.
      assertThat(
          RecurringExchangeReader()
            .readExternalId(databaseClient.singleUse(), ExternalId(EXTERNAL_RECURRING_EXCHANGE_ID3))
            .recurringExchange
            .nextExchangeDate
        )
        .isEqualTo(DATE3_UPDATED)
      // Assert that all the ExchangeSteps are created.
      val steps = readAllExchangeStepsInSpanner()
      assertThat(steps).hasSize(EXCHANGE_WORKFLOW3.stepsCount)
      assertThat(steps.filter { it.state == ExchangeStep.State.READY }).hasSize(0)
    }
}
