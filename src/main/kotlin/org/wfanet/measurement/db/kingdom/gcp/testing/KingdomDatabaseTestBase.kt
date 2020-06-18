package org.wfanet.measurement.db.kingdom.gcp.testing

import com.google.cloud.ByteArray
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Value
import java.time.Instant
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.db.gcp.testing.UsingSpannerEmulator
import org.wfanet.measurement.db.gcp.toGcpTimestamp
import org.wfanet.measurement.db.gcp.toProtoBytes
import org.wfanet.measurement.db.gcp.toProtoEnum
import org.wfanet.measurement.db.gcp.toProtoJson
import org.wfanet.measurement.db.gcp.toSpannerByteArray
import org.wfanet.measurement.db.kingdom.gcp.ReportReader
import org.wfanet.measurement.db.kingdom.gcp.ScheduleReader
import org.wfanet.measurement.internal.kingdom.RepetitionSpec
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.ReportConfigDetails
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.internal.kingdom.ReportDetails
import org.wfanet.measurement.internal.kingdom.RequisitionDetails
import org.wfanet.measurement.internal.kingdom.RequisitionState

abstract class KingdomDatabaseTestBase :
  UsingSpannerEmulator("/src/main/db/gcp/measurement_provider.sdl") {

  private fun write(mutation: Mutation) {
    spanner.client.write(listOf(mutation))
  }

  // TODO: add AdvertiserDetails proto as input.
  protected fun insertAdvertiser(
    advertiserId: Long,
    externalAdvertiserId: Long
  ) {
    write(
      Mutation.newInsertBuilder("Advertisers")
        .set("AdvertiserId").to(advertiserId)
        .set("ExternalAdvertiserId").to(externalAdvertiserId)
        .set("AdvertiserDetails").to(com.google.cloud.ByteArray.copyFrom(""))
        .set("AdvertiserDetailsJson").to("irrelevant-advertiser-details-json")
        .build()
    )
  }

  // TODO: add ReportConfigDetails proto as input.
  // TODO: add State as input.
  protected fun insertReportConfig(
    advertiserId: Long,
    reportConfigId: Long,
    externalReportConfigId: Long,
    reportConfigDetails: ReportConfigDetails = ReportConfigDetails.getDefaultInstance(),
    numRequisitions: Long = 0
  ) {
    write(
      Mutation.newInsertBuilder("ReportConfigs")
        .set("AdvertiserId").to(advertiserId)
        .set("ReportConfigId").to(reportConfigId)
        .set("ExternalReportConfigId").to(externalReportConfigId)
        .set("NumRequisitions").to(numRequisitions)
        .set("State").to(0)
        .set("ReportConfigDetails").to(reportConfigDetails.toSpannerByteArray())
        .set("ReportConfigDetailsJson").to(reportConfigDetails.toJson())
        .build()
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
    write(
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
  }

  protected fun insertReportConfigCampaign(
    advertiserId: Long,
    reportConfigId: Long,
    dataProviderId: Long,
    campaignId: Long
  ) {
    write(
      Mutation.newInsertBuilder("ReportConfigCampaigns")
        .set("AdvertiserId").to(advertiserId)
        .set("ReportConfigId").to(reportConfigId)
        .set("DataProviderId").to(dataProviderId)
        .set("CampaignId").to(campaignId)
        .build()
    )
  }

  protected fun insertReport(
    advertiserId: Long,
    reportConfigId: Long,
    scheduleId: Long,
    reportId: Long,
    externalReportId: Long,
    state: ReportState,
    createTime: Instant? = null,
    windowStartTime: Instant = Instant.EPOCH,
    windowEndTime: Instant = Instant.EPOCH,
    reportDetails: ReportDetails = ReportDetails.getDefaultInstance()
  ) {
    write(
      Mutation.newInsertBuilder("Reports")
        .set("AdvertiserId").to(advertiserId)
        .set("ReportConfigId").to(reportConfigId)
        .set("ScheduleId").to(scheduleId)
        .set("ReportId").to(reportId)
        .set("ExternalReportId").to(externalReportId)
        .set("CreateTime").to(createTime?.toGcpTimestamp() ?: Value.COMMIT_TIMESTAMP)
        .set("WindowStartTime").to(windowStartTime.toGcpTimestamp())
        .set("WindowEndTime").to(windowEndTime.toGcpTimestamp())
        .set("State").toProtoEnum(state)
        .set("ReportDetails").to(reportDetails.toSpannerByteArray())
        .set("ReportDetailsJson").to(reportDetails.toJson())
        .build()
    )
  }

  protected fun insertDataProvider(
    dataProviderId: Long,
    externalDataProviderId: Long
  ) {
    write(
      Mutation.newInsertBuilder("DataProviders")
        .set("DataProviderId").to(dataProviderId)
        .set("ExternalDataProviderId").to(externalDataProviderId)
        .set("DataProviderDetails").to(ByteArray.copyFrom(""))
        .set("DataProviderDetailsJson").to("")
        .build()
    )
  }

  protected fun insertCampaign(
    dataProviderId: Long,
    campaignId: Long,
    externalCampaignId: Long,
    advertiserId: Long,
    providedCampaignId: String = ""
  ) {
    write(
      Mutation.newInsertBuilder("Campaigns")
        .set("DataProviderId").to(dataProviderId)
        .set("CampaignId").to(campaignId)
        .set("ExternalCampaignId").to(externalCampaignId)
        .set("AdvertiserId").to(advertiserId)
        .set("ProvidedCampaignId").to(providedCampaignId)
        .set("CampaignDetails").to(ByteArray.copyFrom(""))
        .set("CampaignDetailsJson").to("")
        .build()
    )
  }

  protected fun insertRequisition(
    dataProviderId: Long,
    campaignId: Long,
    requisitionId: Long,
    externalRequisitionId: Long,
    createTime: Instant = Instant.EPOCH,
    windowStartTime: Instant = Instant.EPOCH,
    windowEndTime: Instant = Instant.EPOCH,
    state: RequisitionState = RequisitionState.UNFULFILLED,
    requisitionDetails: RequisitionDetails = RequisitionDetails.getDefaultInstance()
  ) {
    write(
      Mutation.newInsertBuilder("Requisitions")
        .set("DataProviderId").to(dataProviderId)
        .set("CampaignId").to(campaignId)
        .set("RequisitionId").to(requisitionId)
        .set("ExternalRequisitionId").to(externalRequisitionId)
        .set("CreateTime").to(createTime.toGcpTimestamp())
        .set("WindowStartTime").to(windowStartTime.toGcpTimestamp())
        .set("WindowEndTime").to(windowEndTime.toGcpTimestamp())
        .set("State").toProtoEnum(state)
        .set("RequisitionDetails").toProtoBytes(requisitionDetails)
        .set("RequisitionDetailsJson").toProtoJson(requisitionDetails)
        .build()
    )
  }

  protected fun readAllReportsInSpanner(): List<Report> = runBlocking {
    ReportReader()
      .execute(spanner.client.singleUse())
      .map { it.report }
      .toList()
  }

  protected fun readAllSchedulesInSpanner(): List<ReportConfigSchedule> = runBlocking {
    ScheduleReader()
      .execute(spanner.client.singleUse())
      .map { it.schedule }
      .toList()
  }

  // TODO: add helpers for other tables.
}
