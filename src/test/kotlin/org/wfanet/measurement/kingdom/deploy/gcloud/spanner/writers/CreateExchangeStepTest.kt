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
import kotlin.test.assertFails
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.gcloud.common.toCloudDate
import org.wfanet.measurement.gcloud.spanner.makeStruct
import org.wfanet.measurement.gcloud.spanner.set
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

@RunWith(JUnit4::class)
class CreateExchangeStepTest : KingdomDatabaseTestBase() {
  private fun ExchangeStep.toStruct(
    recurringExchangeId: Long,
    modelProviderId: Long? = null,
    dataProviderId: Long? = null
  ): Struct {
    return makeStruct {
      set("RecurringExchangeId" to recurringExchangeId)
      set("Date" to date.toCloudDate())
      set("StepIndex" to stepIndex.toLong())
      set("State" to state)
      set("ModelProviderId" to modelProviderId)
      set("DataProviderId" to dataProviderId)
    }
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
  }

  private suspend fun readExchangeStepStructs(): List<Struct> =
    databaseClient
      .singleUse(TimestampBound.strong())
      .executeQuery(
        Statement.of(
          """
            SELECT RecurringExchangeId,
                   Date,
                   StepIndex,
                   State,
                   ModelProviderId,
                   DataProviderId
            FROM ExchangeSteps
            """.trimIndent()
        )
      )
      .toList()

  @Test
  fun success() =
    runBlocking<Unit> {
      val exchangeStep =
        ExchangeStep.newBuilder()
          .apply {
            this.externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID1
            this.externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
            this.date = DATE1
            this.state = ExchangeStep.State.READY
            this.stepIndex = STEP_INDEX.toInt()
          }
          .build()

      val actual =
        CreateExchangeStep(
            exchangeStep = exchangeStep,
            recurringExchangeId = RECURRING_EXCHANGE_ID1,
            dataProviderId = DATA_PROVIDER_ID
          )
          .execute(databaseClient)

      assertThat(actual).comparingExpectedFieldsOnly().isEqualTo(exchangeStep)

      assertThat(readExchangeStepStructs())
        .containsExactly(
          exchangeStep.toStruct(
            recurringExchangeId = RECURRING_EXCHANGE_ID1,
            dataProviderId = DATA_PROVIDER_ID
          )
        )
    }

  @Test
  fun `invalid recurring exchange id`() = runBlocking {
    val exchangeStep =
      ExchangeStep.newBuilder()
        .apply {
          this.externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID1
          this.externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          this.date = DATE1
          this.state = ExchangeStep.State.READY
          this.stepIndex = STEP_INDEX.toInt()
        }
        .build()

    assertFails {
      CreateExchangeStep(
          exchangeStep = exchangeStep,
          recurringExchangeId = 1234L,
          dataProviderId = DATA_PROVIDER_ID
        )
        .execute(databaseClient)
    }

    assertThat(readExchangeStepStructs()).isEmpty()
  }

  @Test
  fun `multiple exchange steps`() =
    runBlocking<Unit> {
      val exchangeStep =
        ExchangeStep.newBuilder()
          .apply {
            this.externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID1
            this.externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
            this.date = DATE1
            this.state = ExchangeStep.State.READY
            this.stepIndex = STEP_INDEX.toInt()
          }
          .build()
      val exchangeStep2 =
        ExchangeStep.newBuilder()
          .apply {
            this.externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID2
            this.externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
            this.date = DATE2
            this.state = ExchangeStep.State.READY_FOR_RETRY
            this.stepIndex = STEP_INDEX.toInt()
          }
          .build()
      CreateExchangeStep(
          exchangeStep = exchangeStep,
          recurringExchangeId = RECURRING_EXCHANGE_ID1,
          dataProviderId = DATA_PROVIDER_ID
        )
        .execute(databaseClient)

      CreateExchangeStep(
          exchangeStep = exchangeStep2,
          recurringExchangeId = RECURRING_EXCHANGE_ID2,
          modelProviderId = MODEL_PROVIDER_ID
        )
        .execute(databaseClient)

      assertThat(readExchangeStepStructs())
        .containsExactly(
          exchangeStep2.toStruct(
            recurringExchangeId = RECURRING_EXCHANGE_ID2,
            modelProviderId = MODEL_PROVIDER_ID
          ),
          exchangeStep.toStruct(
            recurringExchangeId = RECURRING_EXCHANGE_ID1,
            dataProviderId = DATA_PROVIDER_ID
          )
        )
    }
}
