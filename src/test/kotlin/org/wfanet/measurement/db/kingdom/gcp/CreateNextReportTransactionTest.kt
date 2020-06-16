package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.TransactionContext
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import java.time.Instant
import java.time.Period
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.InternalId
import org.wfanet.measurement.common.RandomIdGenerator
import org.wfanet.measurement.common.testing.TestClockWithNamedInstants
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.db.gcp.runReadWriteTransaction
import org.wfanet.measurement.db.kingdom.gcp.testing.KingdomDatabaseTestBase
import org.wfanet.measurement.internal.kingdom.RepetitionSpec
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.ReportConfigDetails
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.internal.kingdom.ReportDetails
import org.wfanet.measurement.internal.kingdom.TimePeriod

@RunWith(JUnit4::class)
class CreateNextReportTransactionTest : KingdomDatabaseTestBase() {
  companion object {
    const val ADVERTISER_ID = 1L
    const val REPORT_CONFIG_ID = 2L
    const val SCHEDULE_ID = 3L
    const val EXTERNAL_ADVERTISER_ID = 4L
    const val EXTERNAL_REPORT_CONFIG_ID = 5L
    const val EXTERNAL_SCHEDULE_ID = 6L
    const val REPORT_ID = 7L
    const val EXTERNAL_REPORT_ID = 8L
  }

  private val clock = TestClockWithNamedInstants(Instant.now())

  object FakeIdGenerator : RandomIdGenerator {
    override fun generateInternalId(): InternalId = InternalId(REPORT_ID)
    override fun generateExternalId(): ExternalId = ExternalId(EXTERNAL_REPORT_ID)
  }

  private fun execute(externalScheduleId: ExternalId) {
    spanner.client.runReadWriteTransaction { transactionContext: TransactionContext ->
      CreateNextReportTransaction(clock, FakeIdGenerator)
        .execute(transactionContext, externalScheduleId)
    }
  }

  @Before
  fun populateDatabase() {
    insertAdvertiser(ADVERTISER_ID, EXTERNAL_ADVERTISER_ID)
    insertReportConfig(
      ADVERTISER_ID,
      REPORT_CONFIG_ID,
      EXTERNAL_REPORT_CONFIG_ID,
      ReportConfigDetails.newBuilder().apply {
        reportDurationBuilder.apply {
          unit = TimePeriod.Unit.DAY
          count = 3
        }
      }.build()
    )
  }

  @Test
  fun `nextReportStartTime in the future`() {
    clock.tickSeconds("now")

    insertReportConfigSchedule(
      advertiserId = ADVERTISER_ID,
      reportConfigId = REPORT_CONFIG_ID,
      scheduleId = SCHEDULE_ID,
      externalScheduleId = EXTERNAL_SCHEDULE_ID,
      nextReportStartTime = clock.instant().plus(Period.ofDays(5))
    )

    execute(ExternalId(EXTERNAL_SCHEDULE_ID))

    assertThat(readAllReportsInSpanner()).isEmpty()
  }

  @Test
  fun success() {
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

    execute(ExternalId(EXTERNAL_SCHEDULE_ID))

    val expectedReport: Report = Report.newBuilder().apply {
      externalAdvertiserId = EXTERNAL_ADVERTISER_ID
      externalReportConfigId = EXTERNAL_REPORT_CONFIG_ID
      externalScheduleId = EXTERNAL_SCHEDULE_ID
      externalReportId = EXTERNAL_REPORT_ID
      windowStartTime = clock["nextReportStartTime"].toProtoTime()
      windowEndTime = clock["nextReportStartTime"].plus(Period.ofDays(3)).toProtoTime()
      state = ReportState.AWAITING_REQUISITION_CREATION
      reportDetails = ReportDetails.getDefaultInstance()
      reportDetailsJson = reportDetails.toJson()
    }.build()

    assertThat(readAllReportsInSpanner())
      .comparingExpectedFieldsOnly()
      .containsExactly(expectedReport)

    val expectedSchedule: ReportConfigSchedule = ReportConfigSchedule.newBuilder().apply {
      externalAdvertiserId = EXTERNAL_ADVERTISER_ID
      externalReportConfigId = EXTERNAL_REPORT_CONFIG_ID
      externalScheduleId = EXTERNAL_SCHEDULE_ID
      repetitionSpec = scheduleRepetitionSpec
      repetitionSpecJson = scheduleRepetitionSpec.toJson()
      nextReportStartTime = clock["nextReportStartTime"].plus(Period.ofDays(14)).toProtoTime()
    }.build()

    assertThat(readAllSchedulesInSpanner())
      .comparingExpectedFieldsOnly()
      .containsExactly(expectedSchedule)
  }
}
