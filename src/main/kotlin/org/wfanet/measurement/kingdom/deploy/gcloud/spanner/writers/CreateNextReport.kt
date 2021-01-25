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
import com.google.cloud.spanner.Value
import java.time.Duration
import java.time.Period
import java.time.temporal.TemporalAmount
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.bufferTo
import org.wfanet.measurement.gcloud.spanner.toProtoBytes
import org.wfanet.measurement.gcloud.spanner.toProtoEnum
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.internal.kingdom.ReportDetails
import org.wfanet.measurement.internal.kingdom.TimePeriod
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.ReadLatestReportBySchedule
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ScheduleReader

/**
 * Creates the next [Report] for a [ReportConfigSchedule].
 *
 * It will create a new report if the schedule's nextReportStartTime is in the past, or if there are
 * no reports for this schedule yet.
 */
class CreateNextReport(
  private val externalScheduleId: ExternalId,
  private val combinedPublicKeyResourceId: String
) : SpannerWriter<Report, Report>() {

  override suspend fun TransactionScope.runTransaction(): Report {
    val scheduleReadResult = ScheduleReader().readExternalId(transactionContext, externalScheduleId)
    if (needsNewReport(scheduleReadResult.schedule)) {
      return createNewReport(scheduleReadResult)
    }
    return ReadLatestReportBySchedule(externalScheduleId).executeSingle(transactionContext)
  }

  override fun ResultScope<Report>.buildResult(): Report {
    return checkNotNull(transactionResult).toBuilder().apply {
      createTime = commitTimestamp.toProto()
      updateTime = commitTimestamp.toProto()
    }.build()
  }

  private fun TransactionScope.needsNewReport(schedule: ReportConfigSchedule): Boolean {
    return schedule.nextReportStartTime.toInstant() < clock.instant() ||
      schedule.nextReportStartTime.toInstant() <= schedule.repetitionSpec.start.toInstant()
  }

  private fun TransactionScope.createNewReport(scheduleReadResult: ScheduleReader.Result): Report {
    val schedule: ReportConfigSchedule = scheduleReadResult.schedule

    val repetitionPeriod = getTemporalAmount(schedule.repetitionSpec.repetitionPeriod)
    val reportDuration = getTemporalAmount(scheduleReadResult.reportConfigDetails.reportDuration)

    val windowStartTime = schedule.nextReportStartTime.toInstant()
    val windowEndTime = windowStartTime.plus(reportDuration)

    val nextNextReportStartTime = windowStartTime.plus(repetitionPeriod)

    val reportDetails = ReportDetails.newBuilder().also {
      it.combinedPublicKeyResourceId = combinedPublicKeyResourceId
    }.build()
    val reportDetailsJson: String = reportDetails.toJson()

    Mutation.newUpdateBuilder("ReportConfigSchedules")
      .set("AdvertiserId").to(scheduleReadResult.advertiserId)
      .set("ReportConfigId").to(scheduleReadResult.reportConfigId)
      .set("ScheduleId").to(scheduleReadResult.scheduleId)
      .set("NextReportStartTime").to(nextNextReportStartTime.toGcloudTimestamp())
      .build()
      .bufferTo(transactionContext)

    val newExternalReportId = idGenerator.generateExternalId()
    Mutation.newInsertBuilder("Reports")
      .set("AdvertiserId").to(scheduleReadResult.advertiserId)
      .set("ReportConfigId").to(scheduleReadResult.reportConfigId)
      .set("ScheduleId").to(scheduleReadResult.scheduleId)
      .set("ReportId").to(idGenerator.generateInternalId().value)
      .set("ExternalReportId").to(newExternalReportId.value)
      .set("CreateTime").to(Value.COMMIT_TIMESTAMP)
      .set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
      .set("WindowStartTime").to(windowStartTime.toGcloudTimestamp())
      .set("WindowEndTime").to(windowEndTime.toGcloudTimestamp())
      .set("State").toProtoEnum(ReportState.AWAITING_REQUISITION_CREATION)
      .set("ReportDetails").toProtoBytes(reportDetails)
      .set("ReportDetailsJson").to(reportDetailsJson)
      .build()
      .bufferTo(transactionContext)

    return Report.newBuilder().apply {
      externalAdvertiserId = scheduleReadResult.schedule.externalAdvertiserId
      externalReportConfigId = scheduleReadResult.schedule.externalReportConfigId
      externalScheduleId = scheduleReadResult.schedule.externalScheduleId
      externalReportId = newExternalReportId.value
      this.windowStartTime = windowStartTime.toProtoTime()
      this.windowEndTime = windowEndTime.toProtoTime()
      state = ReportState.AWAITING_REQUISITION_CREATION
      this.reportDetails = reportDetails
      this.reportDetailsJson = reportDetailsJson
    }.build()
  }

  private fun getTemporalAmount(period: TimePeriod): TemporalAmount {
    return when (period.unit) {
      TimePeriod.Unit.DAY -> Period.ofDays(period.count.toInt())
      TimePeriod.Unit.HOUR -> Duration.ofHours(period.count)
      else -> error("Unsupported time unit: $period")
    }
  }
}
