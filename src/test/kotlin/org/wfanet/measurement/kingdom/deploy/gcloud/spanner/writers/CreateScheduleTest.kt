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

import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.identity.testing.FixedIdGenerator
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.toProtoBytes
import org.wfanet.measurement.gcloud.spanner.toProtoJson
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.internal.kingdom.TimePeriod
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.KingdomDatabaseTestBase

private const val ADVERTISER_ID = 1L
private const val EXTERNAL_ADVERTISER_ID = 2L
private const val REPORT_CONFIG_ID = 3L
private const val EXTERNAL_REPORT_CONFIG_ID = 4L

private val SCHEDULE: ReportConfigSchedule = ReportConfigSchedule.newBuilder().apply {
  externalReportConfigId = EXTERNAL_REPORT_CONFIG_ID
  repetitionSpecBuilder.apply {
    startBuilder.seconds = 12345
    repetitionPeriodBuilder.apply {
      unit = TimePeriod.Unit.DAY
      count = 7
    }
  }
}.build()

class CreateScheduleTest : KingdomDatabaseTestBase() {
  @Before
  fun populateDatabase() = runBlocking {
    insertAdvertiser(ADVERTISER_ID, EXTERNAL_ADVERTISER_ID)
    insertReportConfig(ADVERTISER_ID, REPORT_CONFIG_ID, EXTERNAL_REPORT_CONFIG_ID)
  }

  private fun createSchedule(
    schedule: ReportConfigSchedule,
    scheduleId: Long,
    externalScheduleId: Long
  ): ReportConfigSchedule = runBlocking {
    val idGenerator = FixedIdGenerator(InternalId(scheduleId), ExternalId(externalScheduleId))
    CreateSchedule(schedule).execute(databaseClient, idGenerator)
  }

  private suspend fun readSchedules(): List<Struct> {
    return databaseClient
      .singleUse()
      .executeQuery(Statement.of("SELECT * FROM ReportConfigSchedules"))
      .toList()
  }

  private fun makeExpectedScheduleStruct(scheduleId: Long, externalScheduleId: Long): Struct {
    return Struct.newBuilder()
      .set("AdvertiserId").to(ADVERTISER_ID)
      .set("ReportConfigId").to(REPORT_CONFIG_ID)
      .set("ScheduleId").to(scheduleId)
      .set("ExternalScheduleId").to(externalScheduleId)
      .set("NextReportStartTime").to(SCHEDULE.repetitionSpec.start.toGcloudTimestamp())
      .set("RepetitionSpec").toProtoBytes(SCHEDULE.repetitionSpec)
      .set("RepetitionSpecJson").toProtoJson(SCHEDULE.repetitionSpec)
      .build()
  }

  @Test
  fun success() = runBlocking<Unit> {
    val schedule = createSchedule(SCHEDULE, 10L, 11L)
    assertThat(schedule)
      .isEqualTo(
        SCHEDULE.toBuilder().apply {
          externalScheduleId = 11L
          nextReportStartTime = repetitionSpec.start
        }.build()
      )
    assertThat(readSchedules())
      .containsExactly(makeExpectedScheduleStruct(10L, 11L))
  }

  @Test
  fun `multiple schedules`() = runBlocking<Unit> {
    createSchedule(SCHEDULE, 10L, 11L)
    createSchedule(SCHEDULE, 12L, 13L)
    createSchedule(SCHEDULE, 14L, 15L)
    assertThat(readSchedules())
      .containsExactly(
        makeExpectedScheduleStruct(10L, 11L),
        makeExpectedScheduleStruct(12L, 13L),
        makeExpectedScheduleStruct(14L, 15L)
      )
  }
}
