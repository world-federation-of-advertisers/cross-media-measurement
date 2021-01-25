// Copyright 2020 The Cross-Media Measurement Authors
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
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.KingdomDatabaseTestBase

private const val ADVERTISER_ID = 1L
private const val REPORT_CONFIG_ID = 2L
private const val SCHEDULE_ID = 3L
private const val EXTERNAL_ADVERTISER_ID = 4L
private const val EXTERNAL_REPORT_CONFIG_ID = 5L
private const val EXTERNAL_SCHEDULE_ID = 6L
private const val REPORT_ID = 7L
private const val EXTERNAL_REPORT_ID = 8L

@RunWith(JUnit4::class)
class UpdateReportStateTest : KingdomDatabaseTestBase() {
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
      ReportState.AWAITING_REQUISITION_CREATION

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

  private fun updateReportState(state: ReportState): Report = runBlocking {
    UpdateReportState(ExternalId(EXTERNAL_REPORT_ID), state).execute(databaseClient)
  }

  private fun assertContainsReportInState(state: ReportState) {
    assertThat(readAllReportsInSpanner())
      .comparingExpectedFieldsOnly()
      .containsExactly(Report.newBuilder().setState(state).build())
  }

  private fun updateReportStateAndAssertSuccess(state: ReportState) {
    val report = updateReportState(state)
    assertContainsReportInState(state)

    assertThat(report)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        Report.newBuilder().apply {
          externalAdvertiserId = EXTERNAL_ADVERTISER_ID
          externalReportConfigId = EXTERNAL_REPORT_CONFIG_ID
          externalScheduleId = EXTERNAL_SCHEDULE_ID
          externalReportId = EXTERNAL_REPORT_ID
          this.state = state
        }.build()
      )
  }

  @Test
  fun `state update in normal flow`() = runBlocking {
    directlyUpdateState(ReportState.AWAITING_REQUISITION_CREATION)
    updateReportStateAndAssertSuccess(ReportState.AWAITING_DUCHY_CONFIRMATION)
    updateReportStateAndAssertSuccess(ReportState.SUCCEEDED)
  }

  @Test
  fun `terminal states do not allow updates`() = runBlocking {
    val terminalStates = setOf(
      ReportState.REPORT_STATE_UNKNOWN,
      ReportState.FAILED,
      ReportState.CANCELLED,
      ReportState.SUCCEEDED
    )
    for (terminalState in terminalStates) {
      directlyUpdateState(terminalState)
      assertFails { updateReportState(ReportState.IN_PROGRESS) }
    }
  }

  @Test
  fun `noop update`() = runBlocking {
    directlyUpdateState(ReportState.SUCCEEDED)

    // Does not fail:
    updateReportStateAndAssertSuccess(ReportState.SUCCEEDED)
  }
}
