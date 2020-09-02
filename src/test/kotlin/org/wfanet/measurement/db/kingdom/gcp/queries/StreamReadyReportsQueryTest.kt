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

package org.wfanet.measurement.db.kingdom.gcp.queries

import com.google.cloud.spanner.Mutation
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.db.kingdom.gcp.testing.KingdomDatabaseTestBase
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState

@RunWith(JUnit4::class)
class StreamReadyReportsQueryTest : KingdomDatabaseTestBase() {
  companion object {
    const val ADVERTISER_ID = 1L
    const val REPORT_CONFIG_ID = 2L
    const val SCHEDULE_ID = 3L
    const val EXTERNAL_ADVERTISER_ID = 4L
    const val EXTERNAL_REPORT_CONFIG_ID = 5L
    const val EXTERNAL_SCHEDULE_ID = 6L
    const val REPORT_ID = 7L
    const val EXTERNAL_REPORT_ID = 8L
    const val DATA_PROVIDER_ID = 9L
    const val EXTERNAL_DATA_PROVIDER_ID = 10L
    const val CAMPAIGN_ID = 11L
    const val EXTERNAL_CAMPAIGN_ID = 12L
    const val REQUISITION_ID = 13L
    const val EXTERNAL_REQUISITION_ID = 14L
  }

  private fun streamReadyReportsToList(): List<Report> =
    runBlocking {
      StreamReadyReportsQuery()
        .execute(databaseClient.singleUse(), 100L)
        .toList()
    }

  @Before
  fun populateDatabase() {
    insertAdvertiser(
      ADVERTISER_ID,
      EXTERNAL_ADVERTISER_ID
    )
    insertReportConfig(
      ADVERTISER_ID,
      REPORT_CONFIG_ID,
      EXTERNAL_REPORT_CONFIG_ID,
      numRequisitions = 1
    )
    insertReportConfigSchedule(
      advertiserId = ADVERTISER_ID,
      reportConfigId = REPORT_CONFIG_ID,
      scheduleId = SCHEDULE_ID,
      externalScheduleId = EXTERNAL_SCHEDULE_ID
    )

    insertDataProvider(
      DATA_PROVIDER_ID,
      EXTERNAL_DATA_PROVIDER_ID
    )

    insertCampaign(DATA_PROVIDER_ID, CAMPAIGN_ID, EXTERNAL_CAMPAIGN_ID, ADVERTISER_ID)
  }

  private fun insertReportInState(state: ReportState) {
    insertReport(
      ADVERTISER_ID,
      REPORT_CONFIG_ID,
      SCHEDULE_ID,
      REPORT_ID,
      EXTERNAL_REPORT_ID,
      state = state
    )
  }

  private fun insertRequisitionInState(state: RequisitionState) {
    insertRequisition(
      DATA_PROVIDER_ID,
      CAMPAIGN_ID,
      REQUISITION_ID, EXTERNAL_REQUISITION_ID, state = state
    )
  }

  private fun insertReportRequisition() {
    databaseClient.write(
      listOf(
        Mutation.newInsertBuilder("ReportRequisitions")
          .set("AdvertiserId").to(ADVERTISER_ID)
          .set("ReportConfigId").to(REPORT_CONFIG_ID)
          .set("ScheduleId").to(SCHEDULE_ID)
          .set("ReportId").to(REPORT_ID)
          .set("DataProviderId").to(DATA_PROVIDER_ID)
          .set("CampaignId").to(CAMPAIGN_ID)
          .set("RequisitionId").to(REQUISITION_ID)
          .build()
      )
    )
  }

  @Test
  fun success() = runBlocking<Unit> {
    insertReportInState(ReportState.AWAITING_REQUISITION_CREATION)
    insertRequisitionInState(RequisitionState.FULFILLED)
    insertReportRequisition()

    val expectedReport: Report = Report.newBuilder().setExternalReportId(EXTERNAL_REPORT_ID).build()

    assertThat(streamReadyReportsToList())
      .comparingExpectedFieldsOnly()
      .containsExactly(expectedReport)
  }

  @Test
  fun `ignores Reports missing ReportRequisitions`() = runBlocking<Unit> {
    insertReportInState(ReportState.AWAITING_REQUISITION_CREATION)
    insertRequisitionInState(RequisitionState.FULFILLED)
    assertThat(streamReadyReportsToList()).isEmpty()
  }

  @Test
  fun `ignores Reports in other states`() = runBlocking<Unit> {
    insertReportInState(ReportState.AWAITING_DUCHY_CONFIRMATION)
    insertRequisitionInState(RequisitionState.FULFILLED)
    assertThat(streamReadyReportsToList()).isEmpty()
  }

  @Test
  fun `ignores Reports with unfulfilled Requisitions`() = runBlocking<Unit> {
    insertReportInState(ReportState.AWAITING_REQUISITION_CREATION)
    insertRequisitionInState(RequisitionState.UNFULFILLED)
    insertReportRequisition()

    assertThat(streamReadyReportsToList()).isEmpty()
  }
}
