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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.cloud.spanner.Mutation
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import kotlin.test.assertFails
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.spanner.toProtoEnum
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.ReportDetails
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.KingdomDatabaseTestBase

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
    putFrequency(1, 0.4)
    putFrequency(3, 0.6)
  }.build()

@RunWith(JUnit4::class)
class FinishReportTest : KingdomDatabaseTestBase() {
  @Before
  fun populateDatabase() = runBlocking {
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

  private suspend fun directlyUpdateState(state: ReportState) {
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

  private suspend fun finishReport(): Report {
    return FinishReport(ExternalId(EXTERNAL_REPORT_ID), RESULT).execute(databaseClient)
  }

  @Test
  fun `report is in the wrong state`() = runBlocking {
    val states = listOf(
      ReportState.AWAITING_REQUISITION_CREATION,
      ReportState.SUCCEEDED,
      ReportState.FAILED
    )
    for (state in states) {
      directlyUpdateState(state)
      assertFails { finishReport() }
    }
  }

  @Test
  fun success() = runBlocking<Unit> {
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

    assertThat(finishReport())
      .comparingExpectedFieldsOnly()
      .isEqualTo(expectedReport)

    assertThat(readAllReportsInSpanner())
      .comparingExpectedFieldsOnly()
      .containsExactly(expectedReport)
  }
}
