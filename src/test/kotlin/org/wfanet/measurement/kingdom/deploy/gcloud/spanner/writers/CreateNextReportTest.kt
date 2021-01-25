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

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import java.time.Instant
import java.time.Period
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.identity.testing.FixedIdGenerator
import org.wfanet.measurement.common.testing.TestClockWithNamedInstants
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.RepetitionSpec
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.ReportConfigDetails
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.internal.kingdom.TimePeriod
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.KingdomDatabaseTestBase

private const val ADVERTISER_ID = 1L
private const val REPORT_CONFIG_ID = 2L
private const val SCHEDULE_ID = 3L
private const val EXTERNAL_ADVERTISER_ID = 4L
private const val EXTERNAL_REPORT_CONFIG_ID = 5L
private const val EXTERNAL_SCHEDULE_ID = 6L
private const val REPORT_ID = 7L
private const val EXTERNAL_REPORT_ID = 8L
private const val COMBINED_PUBLIC_KEY_RESOURCE_ID = "combined-public-key-1"

@RunWith(JUnit4::class)
class CreateNextReportTest : KingdomDatabaseTestBase() {
  private val clock = TestClockWithNamedInstants(Instant.now())
  private val idGenerator = FixedIdGenerator(InternalId(REPORT_ID), ExternalId(EXTERNAL_REPORT_ID))

  private fun createNextReport(): Report = runBlocking {
    CreateNextReport(ExternalId(EXTERNAL_SCHEDULE_ID), COMBINED_PUBLIC_KEY_RESOURCE_ID).execute(
      databaseClient,
      idGenerator,
      clock
    )
  }

  @Before
  fun populateDatabase() = runBlocking {
    insertAdvertiser(ADVERTISER_ID, EXTERNAL_ADVERTISER_ID)
    insertReportConfig(
      ADVERTISER_ID,
      REPORT_CONFIG_ID,
      EXTERNAL_REPORT_CONFIG_ID,
      reportConfigDetails = ReportConfigDetails.newBuilder().apply {
        reportDurationBuilder.apply {
          unit = TimePeriod.Unit.DAY
          count = 3
        }
      }.build()
    )
  }

  @Test
  fun success() = runBlocking<Unit> {
    clock.tickSeconds("nextReportStartTime")
    clock.tickSeconds("now")

    val scheduleRepetitionSpec: RepetitionSpec = RepetitionSpec.newBuilder().apply {
      repetitionPeriodBuilder.apply {
        unit = TimePeriod.Unit.DAY
        count = 14
      }
    }.build()

    insertReportConfigSchedule(
      advertiserId = ADVERTISER_ID,
      reportConfigId = REPORT_CONFIG_ID,
      scheduleId = SCHEDULE_ID,
      externalScheduleId = EXTERNAL_SCHEDULE_ID,
      nextReportStartTime = clock["nextReportStartTime"],
      repetitionSpec = scheduleRepetitionSpec
    )

    val timestampBefore = currentSpannerTimestamp
    val report = createNextReport()
    val timestampAfter = currentSpannerTimestamp

    val expectedReport: Report = Report.newBuilder().apply {
      externalAdvertiserId = EXTERNAL_ADVERTISER_ID
      externalReportConfigId = EXTERNAL_REPORT_CONFIG_ID
      externalScheduleId = EXTERNAL_SCHEDULE_ID
      externalReportId = EXTERNAL_REPORT_ID
      reportDetailsBuilder.combinedPublicKeyResourceId = COMBINED_PUBLIC_KEY_RESOURCE_ID
      windowStartTime = clock["nextReportStartTime"].toProtoTime()
      windowEndTime = (clock["nextReportStartTime"] + Period.ofDays(3)).toProtoTime()
      state = ReportState.AWAITING_REQUISITION_CREATION
    }.build()

    assertThat(report)
      .comparingExpectedFieldsOnly()
      .isEqualTo(expectedReport)

    assertThat(report.createTime).isEqualTo(report.updateTime)
    assertThat(report.createTime.toInstant()).isGreaterThan(timestampBefore)
    assertThat(report.createTime.toInstant()).isLessThan(timestampAfter)

    assertThat(readAllReportsInSpanner())
      .containsExactly(report)

    val expectedSchedule: ReportConfigSchedule = ReportConfigSchedule.newBuilder().apply {
      externalAdvertiserId = EXTERNAL_ADVERTISER_ID
      externalReportConfigId = EXTERNAL_REPORT_CONFIG_ID
      externalScheduleId = EXTERNAL_SCHEDULE_ID
      repetitionSpec = scheduleRepetitionSpec
      nextReportStartTime = clock["nextReportStartTime"].plus(Period.ofDays(14)).toProtoTime()
    }.build()

    assertThat(readAllSchedulesInSpanner())
      .comparingExpectedFieldsOnly()
      .containsExactly(expectedSchedule)
  }

  @Test
  fun `nextReportStartTime in the future`() = runBlocking<Unit> {
    clock.tickSeconds("now")

    val startTime = clock.instant() + Period.ofDays(5)
    insertReportConfigSchedule(
      advertiserId = ADVERTISER_ID,
      reportConfigId = REPORT_CONFIG_ID,
      scheduleId = SCHEDULE_ID,
      externalScheduleId = EXTERNAL_SCHEDULE_ID,
      nextReportStartTime = startTime,
      repetitionSpec = RepetitionSpec.newBuilder().apply {
        start = startTime.toProtoTime()
        repetitionPeriodBuilder.apply {
          unit = TimePeriod.Unit.DAY
          count = 1
        }
      }.build()
    )

    val report = createNextReport()

    val expectedReport = Report.newBuilder().apply {
      externalAdvertiserId = EXTERNAL_ADVERTISER_ID
      externalReportConfigId = EXTERNAL_REPORT_CONFIG_ID
      externalScheduleId = EXTERNAL_SCHEDULE_ID
      externalReportId = EXTERNAL_REPORT_ID
      windowStartTime = startTime.toProtoTime()
      windowEndTime = (startTime + Period.ofDays(3)).toProtoTime()
      state = ReportState.AWAITING_REQUISITION_CREATION
    }.build()

    assertThat(report)
      .comparingExpectedFieldsOnly()
      .isEqualTo(expectedReport)

    assertThat(readAllReportsInSpanner())
      .comparingExpectedFieldsOnly()
      .containsExactly(report)
  }
}
