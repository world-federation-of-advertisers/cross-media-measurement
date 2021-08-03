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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing

import com.google.cloud.ByteArray
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Value
import com.google.type.Date
import java.time.Instant
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.gcloud.common.toCloudDate
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.insertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.testing.UsingSpannerEmulator
import org.wfanet.measurement.gcloud.spanner.toProtoBytes
import org.wfanet.measurement.gcloud.spanner.toProtoEnum
import org.wfanet.measurement.gcloud.spanner.toProtoJson
import org.wfanet.measurement.internal.kingdom.Exchange
import org.wfanet.measurement.internal.kingdom.ExchangeDetails
import org.wfanet.measurement.internal.kingdom.ExchangeStep
import org.wfanet.measurement.internal.kingdom.RecurringExchange
import org.wfanet.measurement.internal.kingdom.RecurringExchangeDetails
import org.wfanet.measurement.internal.kingdom.RepetitionSpec
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.ReportConfig.ReportConfigState
import org.wfanet.measurement.internal.kingdom.ReportConfigDetails
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.internal.kingdom.ReportDetails
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState
import org.wfanet.measurement.internal.kingdom.RequisitionDetails
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ExchangeStepReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.RecurringExchangeReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ReportReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.RequisitionReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ScheduleReader

private const val COMBINED_PUBLIC_KEY_RESOURCE_ID = "combined-public-key-1"
private val REPORT_DETAILS =
  ReportDetails.newBuilder()
    .apply { combinedPublicKeyResourceId = COMBINED_PUBLIC_KEY_RESOURCE_ID }
    .build()

abstract class KingdomDatabaseTestBase : UsingSpannerEmulator(KINGDOM_LEGACY_SCHEMA) {
  private suspend fun write(mutation: Mutation) = databaseClient.write(mutation)

  // TODO: add AdvertiserDetails proto as input.
  protected suspend fun insertAdvertiser(advertiserId: Long, externalAdvertiserId: Long) {
    write(
      Mutation.newInsertBuilder("Advertisers")
        .set("AdvertiserId")
        .to(advertiserId)
        .set("ExternalAdvertiserId")
        .to(externalAdvertiserId)
        .set("AdvertiserDetails")
        .to(ByteArray.copyFrom(""))
        .set("AdvertiserDetailsJson")
        .to("irrelevant-advertiser-details-json")
        .build()
    )
  }

  protected suspend fun insertReportConfig(
    advertiserId: Long,
    reportConfigId: Long,
    externalReportConfigId: Long,
    state: ReportConfigState = ReportConfigState.ACTIVE,
    reportConfigDetails: ReportConfigDetails = ReportConfigDetails.getDefaultInstance(),
    numRequisitions: Long = 0
  ) {
    write(
      Mutation.newInsertBuilder("ReportConfigs")
        .set("AdvertiserId")
        .to(advertiserId)
        .set("ReportConfigId")
        .to(reportConfigId)
        .set("ExternalReportConfigId")
        .to(externalReportConfigId)
        .set("NumRequisitions")
        .to(numRequisitions)
        .set("State")
        .toProtoEnum(state)
        .set("ReportConfigDetails")
        .toProtoBytes(reportConfigDetails)
        .set("ReportConfigDetailsJson")
        .toProtoJson(reportConfigDetails)
        .build()
    )
  }

  protected suspend fun insertReportConfigSchedule(
    advertiserId: Long,
    reportConfigId: Long,
    scheduleId: Long,
    externalScheduleId: Long,
    nextReportStartTime: Instant = Instant.EPOCH,
    repetitionSpec: RepetitionSpec = RepetitionSpec.getDefaultInstance()
  ) {
    write(
      Mutation.newInsertBuilder("ReportConfigSchedules")
        .set("AdvertiserId")
        .to(advertiserId)
        .set("ReportConfigId")
        .to(reportConfigId)
        .set("ScheduleId")
        .to(scheduleId)
        .set("ExternalScheduleId")
        .to(externalScheduleId)
        .set("NextReportStartTime")
        .to(nextReportStartTime.toGcloudTimestamp())
        .set("RepetitionSpec")
        .toProtoBytes(repetitionSpec)
        .set("RepetitionSpecJson")
        .toProtoJson(repetitionSpec)
        .build()
    )
  }

  protected suspend fun insertReportConfigCampaign(
    advertiserId: Long,
    reportConfigId: Long,
    dataProviderId: Long,
    campaignId: Long
  ) {
    write(
      Mutation.newInsertBuilder("ReportConfigCampaigns")
        .set("AdvertiserId")
        .to(advertiserId)
        .set("ReportConfigId")
        .to(reportConfigId)
        .set("DataProviderId")
        .to(dataProviderId)
        .set("CampaignId")
        .to(campaignId)
        .build()
    )
  }

  protected suspend fun insertReport(
    advertiserId: Long,
    reportConfigId: Long,
    scheduleId: Long,
    reportId: Long,
    externalReportId: Long,
    state: ReportState,
    createTime: Instant? = null,
    updateTime: Instant? = null,
    windowStartTime: Instant = Instant.EPOCH,
    windowEndTime: Instant = Instant.EPOCH,
    reportDetails: ReportDetails = REPORT_DETAILS
  ) {
    write(
      Mutation.newInsertBuilder("Reports")
        .set("AdvertiserId")
        .to(advertiserId)
        .set("ReportConfigId")
        .to(reportConfigId)
        .set("ScheduleId")
        .to(scheduleId)
        .set("ReportId")
        .to(reportId)
        .set("ExternalReportId")
        .to(externalReportId)
        .set("CreateTime")
        .to(createTime?.toGcloudTimestamp() ?: Value.COMMIT_TIMESTAMP)
        .set("UpdateTime")
        .to(updateTime?.toGcloudTimestamp() ?: Value.COMMIT_TIMESTAMP)
        .set("WindowStartTime")
        .to(windowStartTime.toGcloudTimestamp())
        .set("WindowEndTime")
        .to(windowEndTime.toGcloudTimestamp())
        .set("State")
        .toProtoEnum(state)
        .set("ReportDetails")
        .toProtoBytes(reportDetails)
        .set("ReportDetailsJson")
        .toProtoJson(reportDetails)
        .build()
    )
  }

  suspend fun insertReportWithParents(
    advertiserId: Long,
    reportConfigId: Long,
    externalReportConfigId: Long,
    scheduleId: Long,
    externalScheduleId: Long,
    reportId: Long,
    externalReportId: Long,
    state: ReportState,
    createTime: Instant? = null,
    updateTime: Instant? = null,
    windowStartTime: Instant = Instant.EPOCH,
    windowEndTime: Instant = Instant.EPOCH,
    reportDetails: ReportDetails = ReportDetails.getDefaultInstance()
  ) {
    insertReportConfig(advertiserId, reportConfigId, externalReportConfigId)
    insertReportConfigSchedule(advertiserId, reportConfigId, scheduleId, externalScheduleId)
    insertReport(
      advertiserId,
      reportConfigId,
      scheduleId,
      reportId,
      externalReportId,
      state,
      createTime,
      updateTime,
      windowStartTime,
      windowEndTime,
      reportDetails
    )
  }

  protected suspend fun insertDataProvider(dataProviderId: Long, externalDataProviderId: Long) {
    write(
      Mutation.newInsertBuilder("DataProviders")
        .set("DataProviderId")
        .to(dataProviderId)
        .set("ExternalDataProviderId")
        .to(externalDataProviderId)
        .set("DataProviderDetails")
        .to(ByteArray.copyFrom(""))
        .set("DataProviderDetailsJson")
        .to("")
        .build()
    )
  }

  protected suspend fun insertCampaign(
    dataProviderId: Long,
    campaignId: Long,
    externalCampaignId: Long,
    advertiserId: Long,
    providedCampaignId: String = ""
  ) {
    write(
      Mutation.newInsertBuilder("Campaigns")
        .set("DataProviderId")
        .to(dataProviderId)
        .set("CampaignId")
        .to(campaignId)
        .set("ExternalCampaignId")
        .to(externalCampaignId)
        .set("AdvertiserId")
        .to(advertiserId)
        .set("ProvidedCampaignId")
        .to(providedCampaignId)
        .set("CampaignDetails")
        .to(ByteArray.copyFrom(""))
        .set("CampaignDetailsJson")
        .to("")
        .build()
    )
  }

  protected suspend fun insertRequisition(
    dataProviderId: Long,
    campaignId: Long,
    requisitionId: Long,
    externalRequisitionId: Long,
    combinedPublicKeyResourceId: String = COMBINED_PUBLIC_KEY_RESOURCE_ID,
    createTime: Instant = Instant.EPOCH,
    windowStartTime: Instant = Instant.EPOCH,
    windowEndTime: Instant = Instant.EPOCH,
    state: RequisitionState = RequisitionState.UNFULFILLED,
    duchyId: String? = null,
    requisitionDetails: RequisitionDetails = RequisitionDetails.getDefaultInstance()
  ) {
    write(
      Mutation.newInsertBuilder("Requisitions")
        .set("DataProviderId")
        .to(dataProviderId)
        .set("CampaignId")
        .to(campaignId)
        .set("RequisitionId")
        .to(requisitionId)
        .set("ExternalRequisitionId")
        .to(externalRequisitionId)
        .set("CombinedPublicKeyResourceId")
        .to(combinedPublicKeyResourceId)
        .set("CreateTime")
        .to(createTime.toGcloudTimestamp())
        .set("WindowStartTime")
        .to(windowStartTime.toGcloudTimestamp())
        .set("WindowEndTime")
        .to(windowEndTime.toGcloudTimestamp())
        .set("State")
        .toProtoEnum(state)
        .set("DuchyId")
        .to(duchyId)
        .set("RequisitionDetails")
        .toProtoBytes(requisitionDetails)
        .set("RequisitionDetailsJson")
        .toProtoJson(requisitionDetails)
        .build()
    )
  }

  protected suspend fun insertReportRequisition(
    advertiserId: Long,
    reportConfigId: Long,
    scheduleId: Long,
    reportId: Long,
    dataProviderId: Long,
    campaignId: Long,
    requisitionId: Long
  ) {
    write(
      Mutation.newInsertBuilder("ReportRequisitions")
        .set("AdvertiserId")
        .to(advertiserId)
        .set("ReportConfigId")
        .to(reportConfigId)
        .set("ScheduleId")
        .to(scheduleId)
        .set("ReportId")
        .to(reportId)
        .set("DataProviderId")
        .to(dataProviderId)
        .set("CampaignId")
        .to(campaignId)
        .set("RequisitionId")
        .to(requisitionId)
        .build()
    )
  }

  protected suspend fun insertModelProvider(modelProviderId: Long, externalModelProviderId: Long) {
    write(
      insertMutation("ModelProviders") {
        set("modelProviderId" to modelProviderId)
        set("ExternalModelProviderId" to externalModelProviderId)
      }
    )
  }

  suspend fun insertRecurringExchange(
    recurringExchangeId: Long,
    externalRecurringExchangeId: Long,
    modelProviderId: Long,
    dataProviderId: Long,
    state: RecurringExchange.State,
    nextExchangeDate: Date,
    recurringExchangeDetails: RecurringExchangeDetails =
      RecurringExchangeDetails.getDefaultInstance()
  ) {
    write(
      insertMutation("RecurringExchanges") {
        set("RecurringExchangeId" to recurringExchangeId)
        set("ExternalRecurringExchangeId" to externalRecurringExchangeId)
        set("ModelProviderId" to modelProviderId)
        set("DataProviderId" to dataProviderId)
        set("State" to state)
        set("NextExchangeDate" to nextExchangeDate.toCloudDate())
        set("RecurringExchangeDetails" to recurringExchangeDetails)
        set("RecurringExchangeDetailsJson" to recurringExchangeDetails)
      }
    )
  }

  suspend fun insertExchange(
    recurringExchangeId: Long,
    date: Date,
    state: Exchange.State,
    exchangeDetails: ExchangeDetails = ExchangeDetails.getDefaultInstance()
  ) {
    write(
      insertMutation("Exchanges") {
        set("RecurringExchangeId" to recurringExchangeId)
        set("Date" to date.toCloudDate())
        set("State" to state)
        set("ExchangeDetails" to exchangeDetails)
        set("ExchangeDetailsJson" to exchangeDetails)
      }
    )
  }

  suspend fun insertExchangeStep(
    recurringExchangeId: Long,
    date: Date,
    stepIndex: Long,
    state: ExchangeStep.State,
    updateTime: Instant? = null,
    modelProviderId: Long? = null,
    dataProviderId: Long? = null
  ) {
    write(
      insertMutation("ExchangeSteps") {
        set("RecurringExchangeId" to recurringExchangeId)
        set("Date" to date.toCloudDate())
        set("StepIndex" to stepIndex)
        set("State" to state)
        set("UpdateTime" to (updateTime?.toGcloudTimestamp() ?: Value.COMMIT_TIMESTAMP))
        set("ModelProviderId" to modelProviderId)
        set("DataProviderId" to dataProviderId)
      }
    )
  }

  protected fun readAllReportsInSpanner(): List<Report> = runBlocking {
    ReportReader().execute(databaseClient.singleUse()).map { it.report }.toList()
  }

  protected fun readAllSchedulesInSpanner(): List<ReportConfigSchedule> = runBlocking {
    ScheduleReader().execute(databaseClient.singleUse()).map { it.schedule }.toList()
  }

  protected fun readAllRequisitionsInSpanner(): List<Requisition> = runBlocking {
    RequisitionReader().execute(databaseClient.singleUse()).map { it.requisition }.toList()
  }

  protected fun readAllRecurringExchangesInSpanner(): List<RecurringExchange> = runBlocking {
    RecurringExchangeReader()
      .execute(databaseClient.singleUse())
      .map { it.recurringExchange }
      .toList()
  }

  protected fun readAllExchangeStepsInSpanner(): List<ExchangeStep> = runBlocking {
    ExchangeStepReader().execute(databaseClient.singleUse()).map { it.exchangeStep }.toList()
  }

  // TODO: add helpers for other tables.
}
