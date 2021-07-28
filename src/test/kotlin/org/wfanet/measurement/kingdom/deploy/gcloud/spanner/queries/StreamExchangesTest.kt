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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.type.Date
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.internal.kingdom.ExchangeDetails
import org.wfanet.measurement.internal.kingdom.RecurringExchange
import org.wfanet.measurement.internal.kingdom.RecurringExchangeDetails
import org.wfanet.measurement.kingdom.db.StreamExchangesFilter
import org.wfanet.measurement.kingdom.db.streamExchangesFilter
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

private val RECURRING_EXCHANGE_DETAILS = RecurringExchangeDetails.getDefaultInstance()

private val RECURRING_EXCHANGE1 =
  RecurringExchange.newBuilder()
    .apply {
      this.externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID1
      this.externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
      this.externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
      this.nextExchangeDate = DATE1
      this.state = RecurringExchange.State.ACTIVE
      this.details = RECURRING_EXCHANGE_DETAILS
    }
    .build()

private val RECURRING_EXCHANGE2 =
  RecurringExchange.newBuilder()
    .apply {
      this.externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID2
      this.externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID2
      this.externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
      this.nextExchangeDate = DATE2
      this.state = RecurringExchange.State.RETIRED
      this.details = RECURRING_EXCHANGE_DETAILS
    }
    .build()

@RunWith(JUnit4::class)
class StreamExchangesTest : KingdomDatabaseTestBase() {
  private suspend fun executeToList(
    filter: StreamExchangesFilter,
    limit: Long
  ): List<RecurringExchange> {
    return StreamExchanges(filter, limit).execute(databaseClient.singleUse()).toList().map {
      it.recurringExchange
    }
  }

  @Before
  fun populateDatabase() = runBlocking {
    insertDataProvider(DATA_PROVIDER_ID, EXTERNAL_DATA_PROVIDER_ID)
    insertDataProvider(DATA_PROVIDER_ID2, EXTERNAL_DATA_PROVIDER_ID2)
    insertModelProvider(MODEL_PROVIDER_ID, EXTERNAL_MODEL_PROVIDER_ID)

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
      DATA_PROVIDER_ID2,
      RecurringExchange.State.RETIRED,
      DATE2
    )
  }

  @Test
  fun `database sanity check`() {
    assertThat(readAllExchangesInSpanner())
      .comparingExpectedFieldsOnly()
      .containsExactly(RECURRING_EXCHANGE1, RECURRING_EXCHANGE2)
  }

  @Test
  fun `next exchange date filter`() =
    runBlocking<Unit> {
      fun filter(date: Date) = streamExchangesFilter(nextExchangeDate = date)

      assertThat(executeToList(filter(DATE1), 10)).comparingExpectedFieldsOnly().containsExactly()

      assertThat(executeToList(filter(DATE2), 10))
        .comparingExpectedFieldsOnly()
        .containsExactly(RECURRING_EXCHANGE1)

      val date =
        Date.newBuilder()
          .apply {
            year = 2022
            month = 1
            day = 1
          }
          .build()

      assertThat(executeToList(filter(date), 10))
        .comparingExpectedFieldsOnly()
        .containsExactly(RECURRING_EXCHANGE1, RECURRING_EXCHANGE2)
    }

  @Test
  fun `limit filter`() =
    runBlocking<Unit> {
      assertThat(executeToList(streamExchangesFilter(), 10))
        .comparingExpectedFieldsOnly()
        .containsExactly(RECURRING_EXCHANGE1, RECURRING_EXCHANGE2)

      assertThat(executeToList(streamExchangesFilter(), 2))
        .comparingExpectedFieldsOnly()
        .containsExactly(RECURRING_EXCHANGE1, RECURRING_EXCHANGE2)

      assertThat(executeToList(streamExchangesFilter(), 1))
        .comparingExpectedFieldsOnly()
        .containsExactly(RECURRING_EXCHANGE1)
    }

  @Test
  fun `all filters`() = runBlocking {
    val date =
      Date.newBuilder()
        .apply {
          year = 2022
          month = 1
          day = 1
        }
        .build()
    val filter =
      streamExchangesFilter(
        externalModelProviderIds = listOf(ExternalId(EXTERNAL_MODEL_PROVIDER_ID)),
        externalDataProviderIds = listOf(ExternalId(EXTERNAL_DATA_PROVIDER_ID)),
        states = listOf(RecurringExchange.State.ACTIVE),
        nextExchangeDate = date,
      )
    assertThat(executeToList(filter, 10))
      .comparingExpectedFieldsOnly()
      .containsExactly(RECURRING_EXCHANGE1)
      .inOrder()
  }
}
