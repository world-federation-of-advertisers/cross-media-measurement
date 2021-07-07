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
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.identity.testing.FixedIdGenerator
import org.wfanet.measurement.gcloud.common.toCloudDate
import org.wfanet.measurement.gcloud.spanner.toProtoBytes
import org.wfanet.measurement.gcloud.spanner.toProtoEnum
import org.wfanet.measurement.internal.kingdom.Exchange
import org.wfanet.measurement.internal.kingdom.ExchangeDetails
import org.wfanet.measurement.internal.kingdom.RecurringExchange
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.KingdomDatabaseTestBase

private const val DATA_PROVIDER_ID = 1L
private const val EXTERNAL_DATA_PROVIDER_ID = 2L
private const val MODEL_PROVIDER_ID = 3L
private const val EXTERNAL_MODEL_PROVIDER_ID = 4L
private const val RECURRING_EXCHANGE_ID1 = 5L
private const val EXTERNAL_RECURRING_EXCHANGE_ID1 = 6L
private const val RECURRING_EXCHANGE_ID2 = 6L
private const val EXTERNAL_RECURRING_EXCHANGE_ID2 = 7L

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

@RunWith(JUnit4::class)
class CreateExchangeTest : KingdomDatabaseTestBase() {
  private val idGenerator =
    FixedIdGenerator(
      InternalId(RECURRING_EXCHANGE_ID1),
      ExternalId(EXTERNAL_RECURRING_EXCHANGE_ID1)
    )

  private suspend fun createExchange(
    recurringExchangeId: Long,
    externalRecurringExchangeId: Long,
    date: Date,
    state: Exchange.State
  ): Exchange {
    return CreateExchange(recurringExchangeId, ExternalId(externalRecurringExchangeId), date, state)
      .execute(databaseClient, idGenerator)
  }

  private fun makeExpectedExchangeStruct(
    recurringExchangeId: Long,
    date: Date,
    state: Exchange.State
  ): Struct {
    return Struct.newBuilder()
      .set("RecurringExchangeId")
      .to(recurringExchangeId)
      .set("Date")
      .to(date.toCloudDate())
      .set("State")
      .toProtoEnum(state)
      .set("ExchangeDetails")
      .toProtoBytes(
        ExchangeDetails.newBuilder().setAuditTrailHash(ByteString.copyFromUtf8("")).build()
      )
      .set("ExchangeDetailsJson")
      .to("")
      .build()
  }

  @Before
  fun populateDatabase() = runBlocking {
    insertModelProvider(MODEL_PROVIDER_ID, EXTERNAL_MODEL_PROVIDER_ID)
    insertDataProvider(DATA_PROVIDER_ID, EXTERNAL_DATA_PROVIDER_ID)
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
      DATA_PROVIDER_ID,
      RecurringExchange.State.ACTIVE,
      DATE2
    )
  }

  private suspend fun readExchangeStructs(): List<Struct> =
    databaseClient
      .singleUse(TimestampBound.strong())
      .executeQuery(Statement.of("SELECT * FROM Exchanges"))
      .toList()

  @Test
  fun success() =
    runBlocking<Unit> {
      val exchange =
        createExchange(
          RECURRING_EXCHANGE_ID1,
          EXTERNAL_RECURRING_EXCHANGE_ID1,
          DATE1,
          Exchange.State.ACTIVE
        )

      assertThat(exchange)
        .comparingExpectedFieldsOnly()
        .isEqualTo(
          Exchange.newBuilder()
            .apply {
              externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID1
              date = DATE1
              state = Exchange.State.ACTIVE
              details =
                ExchangeDetails.newBuilder().setAuditTrailHash(ByteString.copyFromUtf8("")).build()
              serializedRecurringExchange = ByteString.copyFromUtf8("")
            }
            .build()
        )

      assertThat(readExchangeStructs())
        .containsExactly(
          makeExpectedExchangeStruct(RECURRING_EXCHANGE_ID1, DATE1, Exchange.State.ACTIVE)
        )
    }

  @Test
  fun `invalid recurring exchange id`() = runBlocking {
    assertFails {
      createExchange(1234L, EXTERNAL_RECURRING_EXCHANGE_ID1, DATE1, Exchange.State.ACTIVE)
    }

    assertThat(readExchangeStructs()).isEmpty()
  }

  @Test
  fun `multiple exchanges`() =
    runBlocking<Unit> {
      createExchange(
        RECURRING_EXCHANGE_ID1,
        EXTERNAL_RECURRING_EXCHANGE_ID1,
        DATE1,
        Exchange.State.ACTIVE
      )
      createExchange(
        RECURRING_EXCHANGE_ID2,
        EXTERNAL_RECURRING_EXCHANGE_ID2,
        DATE2,
        Exchange.State.FAILED
      )
      assertThat(readExchangeStructs())
        .containsExactly(
          makeExpectedExchangeStruct(RECURRING_EXCHANGE_ID1, DATE1, Exchange.State.ACTIVE),
          makeExpectedExchangeStruct(RECURRING_EXCHANGE_ID2, DATE2, Exchange.State.FAILED),
        )
    }
}
