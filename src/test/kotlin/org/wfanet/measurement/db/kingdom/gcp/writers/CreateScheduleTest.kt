package org.wfanet.measurement.db.kingdom.gcp.writers

import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import org.junit.Before
import org.junit.Test
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.InternalId
import org.wfanet.measurement.common.testing.FixedIdGenerator
import org.wfanet.measurement.db.gcp.asSequence
import org.wfanet.measurement.db.gcp.toGcpTimestamp
import org.wfanet.measurement.db.gcp.toProtoBytes
import org.wfanet.measurement.db.gcp.toProtoJson
import org.wfanet.measurement.db.kingdom.gcp.testing.KingdomDatabaseTestBase
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.internal.kingdom.TimePeriod

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
  fun populateDatabase() {
    insertAdvertiser(ADVERTISER_ID, EXTERNAL_ADVERTISER_ID)
    insertReportConfig(ADVERTISER_ID, REPORT_CONFIG_ID, EXTERNAL_REPORT_CONFIG_ID)
  }

  private fun createSchedule(
    schedule: ReportConfigSchedule,
    scheduleId: Long,
    externalScheduleId: Long
  ): ReportConfigSchedule {
    val idGenerator = FixedIdGenerator(InternalId(scheduleId), ExternalId(externalScheduleId))
    return CreateSchedule(schedule).execute(databaseClient, idGenerator)
  }

  private fun readSchedules(): List<Struct> {
    return databaseClient
      .singleUse()
      .executeQuery(Statement.of("SELECT * FROM ReportConfigSchedules"))
      .asSequence()
      .toList()
  }

  private fun makeExpectedScheduleStruct(scheduleId: Long, externalScheduleId: Long): Struct {
    return Struct.newBuilder()
      .set("AdvertiserId").to(ADVERTISER_ID)
      .set("ReportConfigId").to(REPORT_CONFIG_ID)
      .set("ScheduleId").to(scheduleId)
      .set("ExternalScheduleId").to(externalScheduleId)
      .set("NextReportStartTime").to(SCHEDULE.repetitionSpec.start.toGcpTimestamp())
      .set("RepetitionSpec").toProtoBytes(SCHEDULE.repetitionSpec)
      .set("RepetitionSpecJson").toProtoJson(SCHEDULE.repetitionSpec)
      .build()
  }

  @Test
  fun success() {
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
  fun `multiple schedules`() {
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
