// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.kingdom

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
import org.wfanet.measurement.internal.kingdom.ReportConfigScheduleStorageGrpcKt.ReportConfigScheduleStorageCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ReportConfigScheduleStorageGrpcKt.ReportConfigScheduleStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReportConfigStorageGrpcKt.ReportConfigStorageCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ReportConfigStorageGrpcKt.ReportConfigStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReportStorageGrpcKt.ReportStorageCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ReportStorageGrpcKt.ReportStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState
import org.wfanet.measurement.internal.kingdom.RequisitionStorageGrpcKt.RequisitionStorageCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.RequisitionStorageGrpcKt.RequisitionStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.RequisitionTemplate
import org.wfanet.measurement.internal.kingdom.StreamReportsRequest
import org.wfanet.measurement.internal.kingdom.UpdateReportStateRequest
import org.wfanet.measurement.service.testing.GrpcTestServerRule

@RunWith(JUnit4::class)
class DaemonDatabaseServicesClientImplTest {

  private val reportConfigStorage: ReportConfigStorageCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())
  private val reportConfigScheduleStorage: ReportConfigScheduleStorageCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())
  private val reportStorage: ReportStorageCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())
  private val requisitionStorage: RequisitionStorageCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    listOf(
      reportConfigStorage,
      reportConfigScheduleStorage,
      reportStorage,
      requisitionStorage
    )
  }

  private val daemonDatabaseServicesClient: DaemonDatabaseServicesClient by lazy {
    val channel = grpcTestServerRule.channel
    DaemonDatabaseServicesClientImpl(
      ReportConfigStorageCoroutineStub(channel),
      ReportConfigScheduleStorageCoroutineStub(channel),
      ReportStorageCoroutineStub(channel),
      RequisitionStorageCoroutineStub(channel)
    )
  }

  @Test
  fun createNextReport() = runBlocking<Unit> {
    val schedule =
      ReportConfigSchedule.newBuilder()
        .setExternalScheduleId(12345)
        .build()

    daemonDatabaseServicesClient.createNextReport(schedule)

    val expectedRequest = CreateNextReportRequest.newBuilder().setExternalScheduleId(12345).build()
    verifyProtoArgument(reportStorage, ReportStorageCoroutineImplBase::createNextReport)
      .isEqualTo(expectedRequest)
  }

  @Test
  fun buildRequisitionsForReport() = runBlocking<Unit> {
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
      ReportConfigStorageCoroutineImplBase::listRequisitionTemplates
    )
      .isEqualTo(expectedRequest)
  }

  @Test
  fun createRequisition() = runBlocking<Unit> {
    val inputRequisition = Requisition.newBuilder().setExternalDataProviderId(1).build()
    val outputRequisition = Requisition.newBuilder().setExternalDataProviderId(2).build()

    whenever(requisitionStorage.createRequisition(any()))
      .thenReturn(outputRequisition)

    assertThat(daemonDatabaseServicesClient.createRequisition(inputRequisition))
      .isEqualTo(outputRequisition)

    verifyProtoArgument(requisitionStorage, RequisitionStorageCoroutineImplBase::createRequisition)
      .isEqualTo(inputRequisition)
  }

  @Test
  fun associateRequisitionToReport() = runBlocking<Unit> {
    val requisition = Requisition.newBuilder().setExternalRequisitionId(1).build()
    val report = Report.newBuilder().setExternalReportId(2).build()

    daemonDatabaseServicesClient.associateRequisitionToReport(requisition, report)

    val expectedRequest = AssociateRequisitionRequest.newBuilder().apply {
      externalRequisitionId = 1
      externalReportId = 2
    }.build()

    verifyProtoArgument(reportStorage, ReportStorageCoroutineImplBase::associateRequisition)
      .isEqualTo(expectedRequest)
  }

  @Test
  fun updateReportState() = runBlocking<Unit> {
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

    verifyProtoArgument(reportStorage, ReportStorageCoroutineImplBase::updateReportState)
      .isEqualTo(expectedRequest)
  }

  @Test
  fun streamReportsInState() = runBlocking<Unit> {
    val report1 = Report.newBuilder().setExternalReportId(1).build()
    val report2 = Report.newBuilder().setExternalReportId(2).build()
    val report3 = Report.newBuilder().setExternalReportId(3).build()

    whenever(reportStorage.streamReports(any()))
      .thenReturn(flowOf(report1, report2, report3))

    val state = ReportState.AWAITING_REQUISITION_FULFILLMENT
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
  fun streamReadyReports() = runBlocking<Unit> {
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
