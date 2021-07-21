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
import org.wfanet.measurement.gcloud.common.toCloudDate
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.updateMutation
import org.wfanet.measurement.internal.kingdom.Exchange
import org.wfanet.measurement.internal.kingdom.ExchangeDetails
import org.wfanet.measurement.internal.kingdom.ExchangeStep
import org.wfanet.measurement.internal.kingdom.RecurringExchange
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
private const val STEP_INDEX = 1L

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
class UpdateExchangeStepStateTest : KingdomDatabaseTestBase() {
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

    insertExchangeStep(
      recurringExchangeId = RECURRING_EXCHANGE_ID1,
      date = DATE1,
      stepIndex = 1L,
      state = ExchangeStep.State.IN_PROGRESS,
      updateTime = Instant.now().minusSeconds(1000),
      modelProviderId = null,
      dataProviderId = DATA_PROVIDER_ID
    )
  }

  private suspend fun directlyUpdateState(state: ExchangeStep.State) {
    databaseClient.write(
      updateMutation("ExchangeSteps") {
        set("RecurringExchangeId" to RECURRING_EXCHANGE_ID1)
        set("Date" to DATE1.toCloudDate())
        set("StepIndex" to STEP_INDEX)
        set("State" to state)
        set("UpdateTime" to Value.COMMIT_TIMESTAMP)
        set("DataProviderId" to DATA_PROVIDER_ID)
      }
    )
  }

  private fun updateExchangeStepState(state: ExchangeStep.State): ExchangeStep = runBlocking {
    UpdateExchangeStepState(RECURRING_EXCHANGE_ID1, DATE1, STEP_INDEX, state)
      .execute(databaseClient)
  }

  private fun assertContainsExchangeStepInState(state: ExchangeStep.State) {
    assertThat(readAllExchangeStepsInSpanner())
      .comparingExpectedFieldsOnly()
      .containsExactly(ExchangeStep.newBuilder().setState(state).build())
  }

  private fun updateStepStateAndAssertSuccess(state: ExchangeStep.State) {
    val exchangeStep = updateExchangeStepState(state)
    assertContainsExchangeStepInState(state)

    assertThat(exchangeStep)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        ExchangeStep.newBuilder()
          .apply {
            externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID1
            externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
            date = DATE1
            stepIndex = STEP_INDEX.toInt()
            this.state = state
            updateTime = Value.COMMIT_TIMESTAMP.toProto()
          }
          .build()
      )
  }

  @Test
  fun `state update in normal flow`() = runBlocking {
    directlyUpdateState(ExchangeStep.State.READY_FOR_RETRY)
    updateStepStateAndAssertSuccess(ExchangeStep.State.READY_FOR_RETRY)
    updateStepStateAndAssertSuccess(ExchangeStep.State.READY)
  }

  @Test
  fun `terminal states do not allow updates`() = runBlocking {
    val terminalStates =
      setOf(
        ExchangeStep.State.BLOCKED,
        ExchangeStep.State.SUCCEEDED,
        ExchangeStep.State.FAILED,
        ExchangeStep.State.STATE_UNSPECIFIED
      )
    for (terminalState in terminalStates) {
      directlyUpdateState(terminalState)
      assertFails { updateExchangeStepState(ExchangeStep.State.IN_PROGRESS) }
    }
  }

  @Test
  fun `noop update`() = runBlocking {
    directlyUpdateState(ExchangeStep.State.SUCCEEDED)

    // Does not fail:
    updateStepStateAndAssertSuccess(ExchangeStep.State.SUCCEEDED)
  }
}
