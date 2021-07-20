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

import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.TimestampBound
import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.type.Date
import java.time.Instant
import kotlin.test.assertFails
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.identity.testing.FixedIdGenerator
import org.wfanet.measurement.gcloud.common.toCloudDate
import org.wfanet.measurement.gcloud.spanner.makeStruct
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.setJson
import org.wfanet.measurement.internal.kingdom.Exchange
import org.wfanet.measurement.internal.kingdom.ExchangeDetails
import org.wfanet.measurement.internal.kingdom.ExchangeStep
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttempt
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptDetails
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
private const val STEP_INDEX2 = 2L

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
private val ATTEMPT_DETAILS = ExchangeStepAttemptDetails.getDefaultInstance()

@RunWith(JUnit4::class)
class CreateExchangeStepAttemptTest : KingdomDatabaseTestBase() {
  private val idGenerator =
    FixedIdGenerator(
      InternalId(RECURRING_EXCHANGE_ID1),
      ExternalId(EXTERNAL_RECURRING_EXCHANGE_ID1)
    )

  private suspend fun createExchangeStepAttempt(
    recurringExchangeId: Long,
    externalRecurringExchangeId: Long,
    date: Date,
    stepIndex: Long
  ): ExchangeStepAttempt {
    return CreateExchangeStepAttempt(
        recurringExchangeId,
        ExternalId(externalRecurringExchangeId),
        date,
        stepIndex
      )
      .execute(databaseClient, idGenerator)
  }

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
      state = ExchangeStep.State.READY,
      updateTime = Instant.now().minusSeconds(1000),
      modelProviderId = null,
      dataProviderId = DATA_PROVIDER_ID
    )
    insertExchangeStep(
      recurringExchangeId = RECURRING_EXCHANGE_ID2,
      date = DATE2,
      stepIndex = 1L,
      state = ExchangeStep.State.BLOCKED,
      updateTime = Instant.now(),
      modelProviderId = null,
      dataProviderId = DATA_PROVIDER_ID
    )
    insertExchangeStep(
      recurringExchangeId = RECURRING_EXCHANGE_ID1,
      date = DATE3,
      stepIndex = 2L,
      state = ExchangeStep.State.READY_FOR_RETRY,
      updateTime = Instant.now(),
      modelProviderId = MODEL_PROVIDER_ID,
      dataProviderId = null
    )
  }

  private suspend fun readAttemptStructs(): List<Struct> =
    databaseClient
      .singleUse(TimestampBound.strong())
      .executeQuery(Statement.of("SELECT * FROM ExchangeStepAttempts"))
      .toList()

  private fun makeExpectedAttemptStruct(
    recurringExchangeId: Long,
    date: Date,
    stepIndex: Long,
    attemptIndex: Long
  ): Struct {
    return makeStruct {
      set("RecurringExchangeId" to recurringExchangeId)
      set("Date" to date.toCloudDate())
      set("StepIndex" to stepIndex)
      set("AttemptIndex" to attemptIndex)
      set("ExchangeStepAttemptDetails" to ATTEMPT_DETAILS)
      setJson("ExchangeStepAttemptDetailsJson" to ATTEMPT_DETAILS)
    }
  }

  @Test
  fun success() =
    runBlocking<Unit> {
      val attempt =
        createExchangeStepAttempt(
          RECURRING_EXCHANGE_ID1,
          EXTERNAL_RECURRING_EXCHANGE_ID1,
          DATE1,
          STEP_INDEX
        )
      assertThat(attempt)
        .isEqualTo(
          ExchangeStepAttempt.newBuilder()
            .apply {
              externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID1
              date = DATE1
              state = ExchangeStepAttempt.State.ACTIVE
              stepIndex = STEP_INDEX.toInt()
              attemptNumber = 1
              details = ATTEMPT_DETAILS
            }
            .build()
        )
      assertThat(readAttemptStructs())
        .containsExactly(makeExpectedAttemptStruct(RECURRING_EXCHANGE_ID1, DATE1, STEP_INDEX, 1L))
    }

  @Test
  fun `invalid step index`() = runBlocking {
    assertFails {
      createExchangeStepAttempt(
        RECURRING_EXCHANGE_ID1,
        EXTERNAL_RECURRING_EXCHANGE_ID1,
        DATE1,
        STEP_INDEX2
      )
    }

    assertThat(readAttemptStructs()).isEmpty()
  }

  @Test
  fun `multiple attempts`() =
    runBlocking<Unit> {
      createExchangeStepAttempt(
        RECURRING_EXCHANGE_ID1,
        EXTERNAL_RECURRING_EXCHANGE_ID1,
        DATE1,
        STEP_INDEX
      )
      createExchangeStepAttempt(
        RECURRING_EXCHANGE_ID1,
        EXTERNAL_RECURRING_EXCHANGE_ID1,
        DATE3,
        STEP_INDEX2
      )
      createExchangeStepAttempt(
        RECURRING_EXCHANGE_ID1,
        EXTERNAL_RECURRING_EXCHANGE_ID1,
        DATE1,
        STEP_INDEX
      )
      createExchangeStepAttempt(
        RECURRING_EXCHANGE_ID1,
        EXTERNAL_RECURRING_EXCHANGE_ID1,
        DATE1,
        STEP_INDEX
      )
      assertThat(readAttemptStructs())
        .containsExactly(
          makeExpectedAttemptStruct(RECURRING_EXCHANGE_ID1, DATE1, STEP_INDEX, 1L),
          makeExpectedAttemptStruct(RECURRING_EXCHANGE_ID1, DATE3, STEP_INDEX2, 1L),
          makeExpectedAttemptStruct(RECURRING_EXCHANGE_ID1, DATE1, STEP_INDEX, 2L),
          makeExpectedAttemptStruct(RECURRING_EXCHANGE_ID1, DATE1, STEP_INDEX, 3L),
        )
    }
}
