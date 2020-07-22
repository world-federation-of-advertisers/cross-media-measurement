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

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import kotlin.test.assertFails
import org.junit.Before
import org.junit.Test
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.db.kingdom.gcp.testing.KingdomDatabaseTestBase
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState

class GetReportQueryTest : KingdomDatabaseTestBase() {
  companion object {
    const val ADVERTISER_ID = 1L
    const val EXTERNAL_ADVERTISER_ID = 2L
    const val REPORT_CONFIG_ID = 3L
    const val EXTERNAL_REPORT_CONFIG_ID = 4L
    const val SCHEDULE_ID = 5L
    const val EXTERNAL_SCHEDULE_ID = 6L
    const val REPORT_ID = 7L
    const val EXTERNAL_REPORT_ID = 8L
  }

  @Before
  fun populateDatabase() {
    insertAdvertiser(ADVERTISER_ID, EXTERNAL_ADVERTISER_ID)
    insertReportConfig(ADVERTISER_ID, REPORT_CONFIG_ID, EXTERNAL_REPORT_CONFIG_ID)
    insertReportConfigSchedule(ADVERTISER_ID, REPORT_CONFIG_ID, SCHEDULE_ID, EXTERNAL_SCHEDULE_ID)
    insertReport(
      ADVERTISER_ID, REPORT_CONFIG_ID, SCHEDULE_ID, REPORT_ID, EXTERNAL_REPORT_ID,
      state = ReportState.READY_TO_START
    )
  }

  @Test
  fun `the Report exists`() {
    val report =
      GetReportQuery()
        .execute(databaseClient.singleUse(), ExternalId(EXTERNAL_REPORT_ID))

    val expected = Report.newBuilder().apply {
      externalAdvertiserId = EXTERNAL_ADVERTISER_ID
      externalReportConfigId = EXTERNAL_REPORT_CONFIG_ID
      externalScheduleId = EXTERNAL_SCHEDULE_ID
      externalReportId = EXTERNAL_REPORT_ID
      state = ReportState.READY_TO_START
    }.build()

    assertThat(report)
      .comparingExpectedFieldsOnly()
      .isEqualTo(expected)
  }

  @Test
  fun `the Report does not exist`() {
    assertFails {
      // Query using the wrong id (the external schedule id) should fail.
      GetReportQuery()
        .execute(databaseClient.singleUse(), ExternalId(EXTERNAL_SCHEDULE_ID))
    }
  }
}
