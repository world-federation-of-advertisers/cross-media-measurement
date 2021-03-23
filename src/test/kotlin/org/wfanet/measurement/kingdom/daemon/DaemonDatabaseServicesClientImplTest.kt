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

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.nhaarman.mockitokotlin2.UseConstructor
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.verify
import com.nhaarman.mockitokotlin2.whenever
import java.time.Instant
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.testing.captureFirst
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.AssociateRequisitionRequest
import org.wfanet.measurement.internal.kingdom.CreateNextReportRequest
import org.wfanet.measurement.internal.kingdom.ListRequisitionTemplatesRequest
import org.wfanet.measurement.internal.kingdom.ListRequisitionTemplatesResponse
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedulesGrpcKt.ReportConfigSchedulesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedulesGrpcKt.ReportConfigSchedulesCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReportConfigsGrpcKt.ReportConfigsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ReportConfigsGrpcKt.ReportConfigsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReportsGrpcKt.ReportsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState
import org.wfanet.measurement.internal.kingdom.RequisitionTemplate
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamReportsRequest
import org.wfanet.measurement.internal.kingdom.UpdateReportStateRequest

@RunWith(JUnit4::class)
class DaemonDatabaseServicesClientImplTest {

  private val reportConfigStorage: ReportConfigsCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())
  private val reportConfigScheduleStorage: ReportConfigSchedulesCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())
  private val reportStorage: ReportsCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())
  private val requisitionStorage: RequisitionsCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(reportConfigStorage)
    addService(reportConfigScheduleStorage)
    addService(reportStorage)
    addService(requisitionStorage)
  }

  private val daemonDatabaseServicesClient: DaemonDatabaseServicesClient by lazy {
    val channel = grpcTestServerRule.channel
    DaemonDatabaseServicesClientImpl(
      ReportConfigsCoroutineStub(channel),
      ReportConfigSchedulesCoroutineStub(channel),
      ReportsCoroutineStub(channel),
      RequisitionsCoroutineStub(channel)
    )
  }

  @Test
  fun createNextReport() = runBlocking {
    val externalScheduleId = 12345L
    val combinedPublicKeyResourceId = "combined-public-key"
    val schedule =
      ReportConfigSchedule.newBuilder()
        .setExternalScheduleId(12345)
        .build()

    daemonDatabaseServicesClient.createNextReport(schedule, combinedPublicKeyResourceId)

    val expectedRequest = CreateNextReportRequest.newBuilder().apply {
      this.externalScheduleId = externalScheduleId
      this.combinedPublicKeyResourceId = combinedPublicKeyResourceId
    }.build()
    verifyProtoArgument(reportStorage, ReportsCoroutineImplBase::createNextReport)
      .isEqualTo(expectedRequest)
  }

  @Test
  fun buildRequisitionsForReport() = runBlocking {
    val requisitionTemplate = RequisitionTemplate.newBuilder().apply {
      externalDataProviderId = 1
      externalCampaignId = 2
      requisitionDetailsBuilder.metricDefinitionBuilder.sketchBuilder.sketchConfigId = 3
    }.build()

    whenever(reportConfigStorage.listRequisitionTemplates(any()))
      .thenReturn(
        ListRequisitionTemplatesResponse.newBuilder()
          .addRequisitionTemplates(requisitionTemplate)
          .build()
      )

    val report = Report.newBuilder()
      .setExternalReportConfigId(4)
      .setWindowStartTime(Instant.ofEpochSecond(123).toProtoTime())
      .setWindowEndTime(Instant.ofEpochSecond(456).toProtoTime())
      .build()

    val expectedRequisition = Requisition.newBuilder().apply {
      externalDataProviderId = 1
      externalCampaignId = 2
      windowStartTime = report.windowStartTime
      windowEndTime = report.windowEndTime
      state = RequisitionState.UNFULFILLED
      requisitionDetails = requisitionTemplate.requisitionDetails
    }.build()

    assertThat(daemonDatabaseServicesClient.buildRequisitionsForReport(report))
      .containsExactly(expectedRequisition)

    val expectedRequest =
      ListRequisitionTemplatesRequest.newBuilder().setExternalReportConfigId(4).build()

    verifyProtoArgument(
      reportConfigStorage,
      ReportConfigsCoroutineImplBase::listRequisitionTemplates
    )
      .isEqualTo(expectedRequest)
  }

  @Test
  fun createRequisition() = runBlocking {
    val inputRequisition = Requisition.newBuilder().setExternalDataProviderId(1).build()
    val outputRequisition = Requisition.newBuilder().setExternalDataProviderId(2).build()

    whenever(requisitionStorage.createRequisition(any()))
      .thenReturn(outputRequisition)

    assertThat(daemonDatabaseServicesClient.createRequisition(inputRequisition))
      .isEqualTo(outputRequisition)

    verifyProtoArgument(requisitionStorage, RequisitionsCoroutineImplBase::createRequisition)
      .isEqualTo(inputRequisition)
  }

  @Test
  fun associateRequisitionToReport() = runBlocking {
    val requisition = Requisition.newBuilder().setExternalRequisitionId(1).build()
    val report = Report.newBuilder().setExternalReportId(2).build()

    daemonDatabaseServicesClient.associateRequisitionToReport(requisition, report)

    val expectedRequest = AssociateRequisitionRequest.newBuilder().apply {
      externalRequisitionId = 1
      externalReportId = 2
    }.build()

    verifyProtoArgument(reportStorage, ReportsCoroutineImplBase::associateRequisition)
      .isEqualTo(expectedRequest)
  }

  @Test
  fun updateReportState() = runBlocking {
    val outputReport = Report.getDefaultInstance()
    whenever(reportStorage.updateReportState(any())).thenReturn(outputReport)

    val report = Report.newBuilder().setExternalReportId(1).build()
    val newState = ReportState.IN_PROGRESS

    daemonDatabaseServicesClient.updateReportState(report, newState)

    val expectedRequest =
      UpdateReportStateRequest.newBuilder()
        .setExternalReportId(report.externalReportId)
        .setState(newState)
        .build()

    verifyProtoArgument(reportStorage, ReportsCoroutineImplBase::updateReportState)
      .isEqualTo(expectedRequest)
  }

  @Test
  fun streamReportsInState() = runBlocking {
    val report1 = Report.newBuilder().setExternalReportId(1).build()
    val report2 = Report.newBuilder().setExternalReportId(2).build()
    val report3 = Report.newBuilder().setExternalReportId(3).build()

    whenever(reportStorage.streamReports(any()))
      .thenReturn(flowOf(report1, report2, report3))

    val state = ReportState.AWAITING_REQUISITION_CREATION
    val outputReports = daemonDatabaseServicesClient.streamReportsInState(state)

    assertThat(outputReports.toList())
      .containsExactly(report1, report2, report3)
      .inOrder()

    val expectedRequest = StreamReportsRequest.newBuilder().apply {
      filterBuilder.addStates(state)
    }.build()

    val actualRequest = captureFirst<StreamReportsRequest> {
      verify(reportStorage).streamReports(capture())
    }
    assertThat(actualRequest)
      .comparingExpectedFieldsOnly()
      .isEqualTo(expectedRequest)
  }

  @Test
  fun streamReadyReports() = runBlocking {
    val report1 = Report.newBuilder().setExternalReportId(1).build()
    val report2 = Report.newBuilder().setExternalReportId(2).build()
    val report3 = Report.newBuilder().setExternalReportId(3).build()

    whenever(reportStorage.streamReadyReports(any()))
      .thenReturn(flowOf(report1, report2, report3))

    val outputReports = daemonDatabaseServicesClient.streamReadyReports()

    assertThat(outputReports.toList())
      .containsExactly(report1, report2, report3)
      .inOrder()
  }

  @Test
  fun streamReadySchedules() = runBlocking<Unit> {
    val schedule1 = ReportConfigSchedule.newBuilder().setExternalScheduleId(1).build()
    val schedule2 = ReportConfigSchedule.newBuilder().setExternalScheduleId(2).build()
    val schedule3 = ReportConfigSchedule.newBuilder().setExternalScheduleId(3).build()

    whenever(reportConfigScheduleStorage.streamReadyReportConfigSchedules(any()))
      .thenReturn(flowOf(schedule1, schedule2, schedule3))

    val outputSchedules = daemonDatabaseServicesClient.streamReadySchedules()

    assertThat(outputSchedules.toList())
      .containsExactly(schedule1, schedule2, schedule3)
  }
}
