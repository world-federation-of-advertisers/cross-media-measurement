package org.wfanet.measurement.db.kingdom.gcp.testing

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Value
import java.time.Instant
import org.wfanet.measurement.common.numberAsLong
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.db.gcp.testing.UsingSpannerEmulator
import org.wfanet.measurement.db.gcp.toGcpTimestamp
import org.wfanet.measurement.db.gcp.toSpannerByteArray
import org.wfanet.measurement.internal.kingdom.RepetitionSpec
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.ReportDetails

abstract class KingdomDatabaseTestBase :
  UsingSpannerEmulator("/src/main/db/gcp/measurement_provider.sdl") {

  // TODO: add AdvertiserDetails proto as input.
  protected fun insertAdvertiser(
    advertiserId: Long,
    externalAdvertiserId: Long
  ) {
    spanner.client.write(
      listOf(
        Mutation.newInsertBuilder("Advertisers")
          .set("AdvertiserId").to(advertiserId)
          .set("ExternalAdvertiserId").to(externalAdvertiserId)
          .set("AdvertiserDetails").to(com.google.cloud.ByteArray.copyFrom(""))
          .set("AdvertiserDetailsJson").to("irrelevant-advertiser-details-json")
          .build()
      )
    )
  }

  // TODO: add ReportConfigDetails proto as input.
  // TODO: add State as input.
  protected fun insertReportConfig(
    advertiserId: Long,
    reportConfigId: Long,
    externalReportConfigId: Long,
    numRequisitions: Long = 0
  ) {
    spanner.client.write(
      listOf(
        Mutation.newInsertBuilder("ReportConfigs")
          .set("AdvertiserId").to(advertiserId)
          .set("ReportConfigId").to(reportConfigId)
          .set("ExternalReportConfigId").to(externalReportConfigId)
          .set("NumRequisitions").to(numRequisitions)
          .set("State").to(0)
          .set("ReportConfigDetails").to(com.google.cloud.ByteArray.copyFrom(""))
          .set("ReportConfigDetailsJson").to("irrelevant-report-config-details-json")
          .build()
      )
    )
  }

  protected fun insertReportConfigSchedule(
    advertiserId: Long,
    reportConfigId: Long,
    scheduleId: Long,
    externalScheduleId: Long,
    nextReportStartTime: Instant = Instant.EPOCH,
    repetitionSpec: RepetitionSpec = RepetitionSpec.getDefaultInstance()
  ) {
    spanner.client.write(
      listOf(
        Mutation.newInsertBuilder("ReportConfigSchedules")
          .set("AdvertiserId").to(advertiserId)
          .set("ReportConfigId").to(reportConfigId)
          .set("ScheduleId").to(scheduleId)
          .set("ExternalScheduleId").to(externalScheduleId)
          .set("NextReportStartTime").to(nextReportStartTime.toGcpTimestamp())
          .set("RepetitionSpec").to(repetitionSpec.toSpannerByteArray())
          .set("RepetitionSpecJson").to(repetitionSpec.toJson())
          .build()
      )
    )
  }

  protected fun insertReport(
    advertiserId: Long,
    reportConfigId: Long,
    scheduleId: Long,
    reportId: Long,
    externalReportId: Long,
    createTime: Instant? = null,
    windowStartTime: Instant = Instant.EPOCH,
    windowEndTime: Instant = Instant.EPOCH,
    state: Report.ReportState = Report.ReportState.AWAITING_REQUISITIONS,
    reportDetails: ReportDetails = ReportDetails.getDefaultInstance()
  ) {
    spanner.client.write(
      listOf(
        Mutation.newInsertBuilder("Reports")
          .set("AdvertiserId").to(advertiserId)
          .set("ReportConfigId").to(reportConfigId)
          .set("ScheduleId").to(scheduleId)
          .set("ReportId").to(reportId)
          .set("ExternalReportId").to(externalReportId)
          .set("CreateTime").to(createTime?.toGcpTimestamp() ?: Value.COMMIT_TIMESTAMP)
          .set("WindowStartTime").to(windowStartTime.toGcpTimestamp())
          .set("WindowEndTime").to(windowEndTime.toGcpTimestamp())
          .set("State").to(state.numberAsLong)
          .set("ReportDetails").to(reportDetails.toSpannerByteArray())
          .set("ReportDetailsJson").to(reportDetails.toJson())
          .build()
      )
    )
  }

  // TODO: add helpers for other tables.
}
