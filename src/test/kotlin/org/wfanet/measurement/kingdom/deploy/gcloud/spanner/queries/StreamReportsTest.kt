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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import java.time.Instant
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.kingdom.db.StreamReportsFilter
import org.wfanet.measurement.kingdom.db.streamReportsFilter
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.KingdomDatabaseTestBase

private const val UNUSED_ID = 999999L
private const val ADVERTISER_ID = 1L
private const val REPORT_CONFIG_ID = 2L
private const val SCHEDULE_ID = 3L
private const val EXTERNAL_ADVERTISER_ID = 4L
private const val EXTERNAL_REPORT_CONFIG_ID = 5L
private const val EXTERNAL_SCHEDULE_ID = 6L
private const val REPORT_ID1 = 7L
private const val REPORT_ID2 = 8L
private const val REPORT_ID3 = 9L
private const val EXTERNAL_REPORT_ID1 = 10L
private const val EXTERNAL_REPORT_ID2 = 11L
private const val EXTERNAL_REPORT_ID3 = 12L

private val REPORT1: Report = Report.newBuilder().apply {
  externalAdvertiserId = EXTERNAL_ADVERTISER_ID
  externalReportConfigId = EXTERNAL_REPORT_CONFIG_ID
  externalScheduleId = EXTERNAL_SCHEDULE_ID
  externalReportId = EXTERNAL_REPORT_ID1
}.build()

private val REPORT2: Report = REPORT1.toBuilder().setExternalReportId(EXTERNAL_REPORT_ID2).build()
private val REPORT3: Report = REPORT1.toBuilder().setExternalReportId(EXTERNAL_REPORT_ID3).build()

@RunWith(JUnit4::class)
class StreamReportsTest : KingdomDatabaseTestBase() {
  private suspend fun executeToList(filter: StreamReportsFilter, limit: Long): List<Report> {
    return StreamReports(filter, limit).execute(databaseClient.singleUse()).toList()
  }

  @Before
  fun populateDatabase() = runBlocking {
    insertAdvertiser(ADVERTISER_ID, EXTERNAL_ADVERTISER_ID)
    insertReportConfig(ADVERTISER_ID, REPORT_CONFIG_ID, EXTERNAL_REPORT_CONFIG_ID)
    insertReportConfigSchedule(ADVERTISER_ID, REPORT_CONFIG_ID, SCHEDULE_ID, EXTERNAL_SCHEDULE_ID)

    suspend fun insertReportWithIds(reportId: Long, externalReportId: Long) =
      insertReport(
        ADVERTISER_ID,
        REPORT_CONFIG_ID,
        SCHEDULE_ID,
        reportId,
        externalReportId,
        state = ReportState.IN_PROGRESS
      )

    insertReportWithIds(REPORT_ID1, EXTERNAL_REPORT_ID1)
    insertReportWithIds(REPORT_ID2, EXTERNAL_REPORT_ID2)
    insertReportWithIds(REPORT_ID3, EXTERNAL_REPORT_ID3)
  }

  @Test
  fun limits() = runBlocking {
    assertThat(executeToList(streamReportsFilter(), 10))
      .comparingExpectedFieldsOnly()
      .containsExactly(REPORT1, REPORT2, REPORT3)
      .inOrder()

    assertThat(executeToList(streamReportsFilter(), 2))
      .comparingExpectedFieldsOnly()
      .containsExactly(REPORT1, REPORT2)
      .inOrder()

    assertThat(executeToList(streamReportsFilter(), 1))
      .comparingExpectedFieldsOnly()
      .containsExactly(REPORT1)

    assertThat(executeToList(streamReportsFilter(), 0))
      .comparingExpectedFieldsOnly()
      .containsExactly(REPORT1, REPORT2, REPORT3)
      .inOrder()
  }

  @Test
  fun `create time`() = runBlocking<Unit> {
    suspend fun executeWithTimeFilter(time: Instant) =
      executeToList(streamReportsFilter(updatedAfter = time), 100)

    val all = executeWithTimeFilter(Instant.EPOCH)

    assertThat(all)
      .comparingExpectedFieldsOnly()
      .containsExactly(REPORT1, REPORT2, REPORT3)
      .inOrder()

    assertThat(executeWithTimeFilter(all[0].createTime.toInstant()))
      .comparingExpectedFieldsOnly()
      .containsExactly(REPORT2, REPORT3)
  }

  @Test
  fun `external id filters`() = runBlocking {
    fun wrongIdIf(condition: Boolean, id: Long) = ExternalId(if (condition) UNUSED_ID else id)

    repeat(3) {
      val filter = streamReportsFilter(
        externalAdvertiserIds = listOf(wrongIdIf(it == 0, EXTERNAL_ADVERTISER_ID)),
        externalReportConfigIds = listOf(wrongIdIf(it == 1, EXTERNAL_REPORT_CONFIG_ID)),
        externalScheduleIds = listOf(wrongIdIf(it == 2, EXTERNAL_SCHEDULE_ID))
      )
      assertThat(executeToList(filter, 10))
        .isEmpty()
    }
  }

  @Test
  fun `all filters`() = runBlocking {
    val filter = streamReportsFilter(
      externalAdvertiserIds = listOf(ExternalId(EXTERNAL_ADVERTISER_ID)),
      externalReportConfigIds = listOf(ExternalId(EXTERNAL_REPORT_CONFIG_ID)),
      externalScheduleIds = listOf(ExternalId(EXTERNAL_SCHEDULE_ID)),
      states = listOf(ReportState.IN_PROGRESS),
      updatedAfter = Instant.EPOCH
    )
    assertThat(executeToList(filter, 10))
      .comparingExpectedFieldsOnly()
      .containsExactly(REPORT1, REPORT2, REPORT3)
      .inOrder()
  }
}
