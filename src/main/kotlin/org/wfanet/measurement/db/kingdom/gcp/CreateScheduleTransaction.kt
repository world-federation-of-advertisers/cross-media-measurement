package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.TransactionContext
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.db.gcp.toGcpTimestamp
import org.wfanet.measurement.db.gcp.toProtoBytes
import org.wfanet.measurement.db.gcp.toProtoJson
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule

class CreateScheduleTransaction(private val idGenerator: IdGenerator) {
  fun execute(
    transactionContext: TransactionContext,
    schedule: ReportConfigSchedule
  ): ReportConfigSchedule {
    val reportConfigReadResult = runBlocking {
      ReportConfigReader()
        .readExternalId(transactionContext, ExternalId(schedule.externalReportConfigId))
    }

    val actualSchedule = schedule.toBuilder().apply {
      externalScheduleId = idGenerator.generateExternalId().value
      nextReportStartTime = repetitionSpec.start
    }.build()

    transactionContext.buffer(actualSchedule.toMutation(reportConfigReadResult))
    return actualSchedule
  }

  private fun ReportConfigSchedule.toMutation(
    reportConfigReadResult: ReportConfigReader.Result
  ): Mutation {
    return Mutation.newInsertBuilder("ReportConfigSchedules")
      .set("AdvertiserId").to(reportConfigReadResult.advertiserId)
      .set("ReportConfigId").to(reportConfigReadResult.reportConfigId)
      .set("ScheduleId").to(idGenerator.generateInternalId().value)
      .set("ExternalScheduleId").to(externalScheduleId)
      .set("NextReportStartTime").to(nextReportStartTime.toGcpTimestamp())
      .set("RepetitionSpec").toProtoBytes(repetitionSpec)
      .set("RepetitionSpecJson").toProtoJson(repetitionSpec)
      .build()
  }
}
