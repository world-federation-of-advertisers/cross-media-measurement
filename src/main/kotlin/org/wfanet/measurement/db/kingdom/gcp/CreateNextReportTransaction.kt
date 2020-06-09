package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.TransactionContext
import com.google.cloud.spanner.Value
import java.time.Clock
import java.time.Duration
import java.time.Period
import java.time.temporal.TemporalAmount
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.RandomIdGenerator
import org.wfanet.measurement.common.numberAsLong
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.db.gcp.appendClause
import org.wfanet.measurement.db.gcp.toGcpTimestamp
import org.wfanet.measurement.db.gcp.toSpannerByteArray
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.internal.kingdom.ReportDetails
import org.wfanet.measurement.internal.kingdom.TimePeriod

class CreateNextReportTransaction(
  private val clock: Clock,
  private val randomIdGenerator: RandomIdGenerator
) {

  fun execute(transactionContext: TransactionContext, externalScheduleId: ExternalId) {
    val scheduleReadResult = runBlocking { readSchedule(transactionContext, externalScheduleId) }
    if (needsNewReport(scheduleReadResult.schedule)) {
      createNewReport(transactionContext, scheduleReadResult)
    }
  }

  private fun needsNewReport(schedule: ReportConfigSchedule): Boolean =
    schedule.nextReportStartTime.toInstant() < clock.instant()

  private suspend fun readSchedule(
    transactionContext: TransactionContext,
    externalScheduleId: ExternalId
  ): ScheduleReadResult {
    val reader = ScheduleReader()
    reader.builder
      .appendClause("WHERE ReportConfigSchedules.ExternalScheduleId = @external_schedule_id")
      .bind("external_schedule_id").to(externalScheduleId.value)
    return reader.execute(transactionContext).single()
  }

  private fun createNewReport(
    transactionContext: TransactionContext,
    scheduleReadResult: ScheduleReadResult
  ) {
    val schedule: ReportConfigSchedule = scheduleReadResult.schedule

    val repetitionPeriod = getTemporalAmount(schedule.repetitionSpec.repetitionPeriod)
    val reportDuration = getTemporalAmount(scheduleReadResult.reportConfigDetails.reportDuration)

    val windowStartTime = schedule.nextReportStartTime.toInstant()
    val windowEndTime = windowStartTime.plus(reportDuration)

    val nextNextReportStartTime = windowStartTime.plus(repetitionPeriod)

    val updateScheduleMutation: Mutation =
      Mutation.newUpdateBuilder("ReportConfigSchedules")
        .set("AdvertiserId").to(scheduleReadResult.advertiserId)
        .set("ReportConfigId").to(scheduleReadResult.reportConfigId)
        .set("ScheduleId").to(scheduleReadResult.scheduleId)
        .set("NextReportStartTime").to(nextNextReportStartTime.toGcpTimestamp())
        .build()

    val insertReportMutation: Mutation =
      Mutation.newInsertBuilder("Reports")
        .set("AdvertiserId").to(scheduleReadResult.advertiserId)
        .set("ReportConfigId").to(scheduleReadResult.reportConfigId)
        .set("ScheduleId").to(scheduleReadResult.scheduleId)
        .set("ReportId").to(randomIdGenerator.generateInternalId().value)
        .set("ExternalReportId").to(randomIdGenerator.generateExternalId().value)
        .set("CreateTime").to(Value.COMMIT_TIMESTAMP)
        .set("WindowStartTime").to(windowStartTime.toGcpTimestamp())
        .set("WindowEndTime").to(windowEndTime.toGcpTimestamp())
        .set("State").to(Report.ReportState.AWAITING_REQUISITIONS.numberAsLong)
        .set("ReportDetails").to(ReportDetails.getDefaultInstance().toSpannerByteArray())
        .set("ReportDetailsJson").to(ReportDetails.getDefaultInstance().toJson())
        .build()

    transactionContext.buffer(listOf(updateScheduleMutation, insertReportMutation))
  }

  private fun getTemporalAmount(period: TimePeriod): TemporalAmount =
    when (period.unit) {
      TimePeriod.Unit.DAY -> Period.ofDays(period.count.toInt())
      TimePeriod.Unit.HOUR -> Duration.ofHours(period.count)
      else -> error("Unsupported time unit: $period")
    }
}
