// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.TransactionContext
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import kotlin.test.assertFails
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.db.gcp.runReadWriteTransaction
import org.wfanet.measurement.db.gcp.toProtoEnum
import org.wfanet.measurement.db.kingdom.gcp.testing.KingdomDatabaseTestBase
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.ReportDetails

private const val ADVERTISER_ID = 1L
private const val REPORT_CONFIG_ID = 2L
private const val SCHEDULE_ID = 3L
private const val EXTERNAL_ADVERTISER_ID = 4L
private const val EXTERNAL_REPORT_CONFIG_ID = 5L
private const val EXTERNAL_SCHEDULE_ID = 6L
private const val REPORT_ID = 7L
private const val EXTERNAL_REPORT_ID = 8L
private val RESULT: ReportDetails.Result =
  ReportDetails.Result.newBuilder().apply {
    reach = 123
    putFrequency(1, 2)
    putFrequency(3, 4)
  }.build()

@RunWith(JUnit4::class)
class FinishReportTransactionTest : KingdomDatabaseTestBase() {
  @Before
  fun populateDatabase() {
    insertAdvertiser(ADVERTISER_ID, EXTERNAL_ADVERTISER_ID)
    insertReportConfig(ADVERTISER_ID, REPORT_CONFIG_ID, EXTERNAL_REPORT_CONFIG_ID)
    insertReportConfigSchedule(ADVERTISER_ID, REPORT_CONFIG_ID, SCHEDULE_ID, EXTERNAL_SCHEDULE_ID)
    insertReport(
      ADVERTISER_ID,
      REPORT_CONFIG_ID,
      SCHEDULE_ID,
      REPORT_ID,
      EXTERNAL_REPORT_ID,
      ReportState.IN_PROGRESS
    )
  }

  private fun directlyUpdateState(state: ReportState) {
    databaseClient.write(
      listOf(
        Mutation.newUpdateBuilder("Reports")
          .set("AdvertiserId").to(ADVERTISER_ID)
          .set("ReportConfigId").to(REPORT_CONFIG_ID)
          .set("ScheduleId").to(SCHEDULE_ID)
          .set("ReportId").to(REPORT_ID)
          .set("State").toProtoEnum(state)
          .build()
      )
    )
  }

  private fun runFinishReportTransaction(): Report {
    return databaseClient.runReadWriteTransaction { transactionContext: TransactionContext ->
      FinishReportTransaction()
        .execute(transactionContext, ExternalId(EXTERNAL_REPORT_ID), RESULT)
    }
  }

  @Test
  fun `report is in the wrong state`() {
    val states = listOf(
      ReportState.AWAITING_REQUISITION_CREATION,
      ReportState.AWAITING_REQUISITION_FULFILLMENT,
      ReportState.SUCCEEDED,
      ReportState.FAILED
    )
    for (state in states) {
      directlyUpdateState(state)
      assertFails { runFinishReportTransaction() }
    }
  }

  @Test
  fun success() {
    directlyUpdateState(ReportState.IN_PROGRESS)

    val expectedReport =
      Report.newBuilder().apply {
        externalAdvertiserId = EXTERNAL_ADVERTISER_ID
        externalReportConfigId = EXTERNAL_REPORT_CONFIG_ID
        externalScheduleId = EXTERNAL_SCHEDULE_ID
        externalReportId = EXTERNAL_REPORT_ID
        state = ReportState.SUCCEEDED
        reportDetailsBuilder.result = RESULT
      }.build()

    assertThat(runFinishReportTransaction())
      .comparingExpectedFieldsOnly()
      .isEqualTo(expectedReport)

    assertThat(readAllReportsInSpanner())
      .comparingExpectedFieldsOnly()
      .containsExactly(expectedReport)
  }
}
