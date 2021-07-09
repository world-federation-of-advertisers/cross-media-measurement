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
import org.wfanet.measurement.internal.kingdom.Exchange
import org.wfanet.measurement.internal.kingdom.ExchangeDetails
import org.wfanet.measurement.internal.kingdom.ExchangeStep
import org.wfanet.measurement.internal.kingdom.RecurringExchange
import org.wfanet.measurement.internal.kingdom.Report.ReportState
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

//private val EXCHANGE_STEP1 = buildExchangeStep(DATE1, Exchange.State.ACTIVE)
//private val EXCHANGE_STEP2 = buildExchangeStep(DATE2, Exchange.State.FAILED)
//private val EXCHANGE_STEP3 = buildExchangeStep(DATE3, Exchange.State.ACTIVE)

@RunWith(JUnit4::class)
class GetExchangeStepTest : KingdomDatabaseTestBase() {
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
    insertExchange(RECURRING_EXCHANGE_ID1, DATE1, Exchange.State.ACTIVE, EXCHANGE_DETAILS)
    insertExchange(RECURRING_EXCHANGE_ID2, DATE2, Exchange.State.FAILED, EXCHANGE_DETAILS)
    insertExchange(RECURRING_EXCHANGE_ID1, DATE3, Exchange.State.ACTIVE, EXCHANGE_DETAILS)
  }

//  @Test
//  fun `the Exchange Step exists`() = runBlocking {
//    val report = GetExchangeStepByDataProvider(ExternalId(EXTERNAL_REPORT_ID)).executeSingle(databaseClient.singleUse())
//
//    val expected =
//      ExchangeStep.newBuilder()
//        .apply {
//          externalAdvertiserId = EXTERNAL_ADVERTISER_ID
//          externalReportConfigId = EXTERNAL_REPORT_CONFIG_ID
//          externalScheduleId = EXTERNAL_SCHEDULE_ID
//          externalReportId = EXTERNAL_REPORT_ID
//          state = ReportState.IN_PROGRESS
//        }
//        .build()
//
//    assertThat(report).comparingExpectedFieldsOnly().isEqualTo(expected)
//  }

  @Test
  fun `the Exchange Step does not exist`() = runBlocking {
    val exchangeSteps =
      GetExchangeStepByModelProvider(ExternalId(EXTERNAL_DATA_PROVIDER_ID), listOf(ExchangeStep.State.READY)).execute(databaseClient.singleUse()).toList()
    assertThat(exchangeSteps).isEmpty()
  }
}
//
//private fun buildExchangeStep(state: Exchange.State): ExchangeStep {
//  return ExchangeStep.newBuilder()
//    .apply {
//      this.date = date
//      this.state = state
//      this.details = EXCHANGE_DETAILS
//    }
//    .build()
//}
