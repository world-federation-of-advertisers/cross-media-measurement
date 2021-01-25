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

package org.wfanet.measurement.kingdom.daemon

import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.internal.kingdom.AssociateRequisitionRequest
import org.wfanet.measurement.internal.kingdom.CreateNextReportRequest
import org.wfanet.measurement.internal.kingdom.ListRequisitionTemplatesRequest
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedulesGrpcKt.ReportConfigSchedulesCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReportConfigsGrpcKt.ReportConfigsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState
import org.wfanet.measurement.internal.kingdom.RequisitionTemplate
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamReadyReportConfigSchedulesRequest
import org.wfanet.measurement.internal.kingdom.StreamReadyReportsRequest
import org.wfanet.measurement.internal.kingdom.StreamReportsRequest
import org.wfanet.measurement.internal.kingdom.UpdateReportStateRequest

class DaemonDatabaseServicesClientImpl(
  private val reportConfigStorage: ReportConfigsCoroutineStub,
  private val reportConfigScheduleStorage: ReportConfigSchedulesCoroutineStub,
  private val reportStorage: ReportsCoroutineStub,
  private val requisitionStorage: RequisitionsCoroutineStub
) : DaemonDatabaseServicesClient {
  override suspend fun createNextReport(
    reportConfigSchedule: ReportConfigSchedule,
    combinedPublicKeyResourceId: String
  ) {
    val request = CreateNextReportRequest.newBuilder().apply {
      externalScheduleId = reportConfigSchedule.externalScheduleId
      this.combinedPublicKeyResourceId = combinedPublicKeyResourceId
    }.build()
    reportStorage.createNextReport(request)
  }

  override suspend fun buildRequisitionsForReport(report: Report): List<Requisition> {
    val request =
      ListRequisitionTemplatesRequest.newBuilder()
        .setExternalReportConfigId(report.externalReportConfigId)
        .build()

    val response = reportConfigStorage.listRequisitionTemplates(request)

    return response.requisitionTemplatesList.map { buildRequisition(report, it) }
  }

  private fun buildRequisition(report: Report, template: RequisitionTemplate): Requisition =
    Requisition.newBuilder().apply {
      externalDataProviderId = template.externalDataProviderId
      externalCampaignId = template.externalCampaignId
      combinedPublicKeyResourceId = report.reportDetails.combinedPublicKeyResourceId
      windowStartTime = report.windowStartTime
      windowEndTime = report.windowEndTime
      state = RequisitionState.UNFULFILLED
      requisitionDetails = template.requisitionDetails
    }.build()

  override suspend fun createRequisition(requisition: Requisition): Requisition =
    requisitionStorage.createRequisition(requisition)

  override suspend fun associateRequisitionToReport(requisition: Requisition, report: Report) {
    reportStorage.associateRequisition(
      AssociateRequisitionRequest.newBuilder()
        .setExternalReportId(report.externalReportId)
        .setExternalRequisitionId(requisition.externalRequisitionId)
        .build()
    )
  }

  override suspend fun updateReportState(report: Report, newState: ReportState) {
    reportStorage.updateReportState(
      UpdateReportStateRequest.newBuilder()
        .setExternalReportId(report.externalReportId)
        .setState(newState)
        .build()
    )
  }

  override fun streamReportsInState(state: ReportState): Flow<Report> =
    reportStorage.streamReports(
      StreamReportsRequest.newBuilder().apply {
        filterBuilder.addStates(state)
      }.build()
    )

  override fun streamReadyReports(): Flow<Report> =
    reportStorage.streamReadyReports(StreamReadyReportsRequest.getDefaultInstance())

  override fun streamReadySchedules(): Flow<ReportConfigSchedule> =
    reportConfigScheduleStorage.streamReadyReportConfigSchedules(
      StreamReadyReportConfigSchedulesRequest.getDefaultInstance()
    )
}
