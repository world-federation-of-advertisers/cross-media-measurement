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

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import java.time.Duration
import java.time.Instant
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.db.kingdom.gcp.testing.KingdomDatabaseTestBase
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule

@RunWith(JUnit4::class)
class StreamReadySchedulesQueryTest : KingdomDatabaseTestBase() {
  companion object {
    const val ADVERTISER_ID = 1L
    const val EXTERNAL_ADVERTISER_ID = 2L

    const val REPORT_CONFIG_ID = 3L
    const val EXTERNAL_REPORT_CONFIG_ID = 4L

    const val SCHEDULE_ID1 = 5L
    const val EXTERNAL_SCHEDULE_ID1 = 6L

    const val SCHEDULE_ID2 = 7L
    const val EXTERNAL_SCHEDULE_ID2 = 8L

    val SCHEDULE1: ReportConfigSchedule = ReportConfigSchedule.newBuilder().apply {
      externalAdvertiserId = EXTERNAL_ADVERTISER_ID
      externalReportConfigId = EXTERNAL_REPORT_CONFIG_ID
      externalScheduleId = EXTERNAL_SCHEDULE_ID1
    }.build()

    val SCHEDULE2: ReportConfigSchedule = ReportConfigSchedule.newBuilder().apply {
      externalAdvertiserId = EXTERNAL_ADVERTISER_ID
      externalReportConfigId = EXTERNAL_REPORT_CONFIG_ID
      externalScheduleId = EXTERNAL_SCHEDULE_ID2
    }.build()
  }

  private fun streamReadySchedulesToList(limit: Long): List<ReportConfigSchedule> =
    runBlocking {
      StreamReadySchedulesQuery()
        .execute(databaseClient.singleUse(), limit)
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
      EXTERNAL_REPORT_CONFIG_ID
    )
  }

  private fun insertSchedule1(time: Instant) {
    insertReportConfigSchedule(
      advertiserId = ADVERTISER_ID,
      reportConfigId = REPORT_CONFIG_ID,
      scheduleId = SCHEDULE_ID1,
      externalScheduleId = EXTERNAL_SCHEDULE_ID1,
      nextReportStartTime = time
    )
  }

  private fun insertSchedule2(time: Instant) {
    insertReportConfigSchedule(
      advertiserId = ADVERTISER_ID,
      reportConfigId = REPORT_CONFIG_ID,
      scheduleId = SCHEDULE_ID2,
      externalScheduleId = EXTERNAL_SCHEDULE_ID2,
      nextReportStartTime = time
    )
  }

  @Test
  fun `filters on time`() = runBlocking<Unit> {
    val past = Instant.now().minus(Duration.ofMinutes(10))
    val future = Instant.now().plus(Duration.ofMinutes(10))

    insertSchedule1(past)
    insertSchedule2(future)

    assertThat(streamReadySchedulesToList(limit = 5))
      .comparingExpectedFieldsOnly()
      .containsExactly(SCHEDULE1)
  }

  @Test
  fun limits() = runBlocking<Unit> {
    insertSchedule1(Instant.EPOCH)
    insertSchedule2(Instant.EPOCH)

    assertThat(streamReadySchedulesToList(limit = 2))
      .comparingExpectedFieldsOnly()
      .containsExactly(SCHEDULE1, SCHEDULE2)

    val limit1Results = streamReadySchedulesToList(limit = 1)
    assertThat(limit1Results)
      .comparingExpectedFieldsOnly()
      .containsAnyOf(SCHEDULE1, SCHEDULE2)
    assertThat(limit1Results).hasSize(1)

    assertThat(streamReadySchedulesToList(limit = 0))
      .comparingExpectedFieldsOnly()
      .containsExactly(SCHEDULE1, SCHEDULE2)
  }
}
