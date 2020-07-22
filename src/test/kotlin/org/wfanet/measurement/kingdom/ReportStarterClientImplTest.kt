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
import java.time.Instant
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.AssociateRequisitionRequest
import org.wfanet.measurement.internal.kingdom.AssociateRequisitionResponse
import org.wfanet.measurement.internal.kingdom.CreateNextReportRequest
import org.wfanet.measurement.internal.kingdom.ListRequisitionTemplatesRequest
import org.wfanet.measurement.internal.kingdom.ListRequisitionTemplatesResponse
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.internal.kingdom.ReportConfigScheduleStorageGrpcKt.ReportConfigScheduleStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReportConfigStorageGrpcKt.ReportConfigStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReportStorageGrpcKt.ReportStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState
import org.wfanet.measurement.internal.kingdom.RequisitionStorageGrpcKt.RequisitionStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.RequisitionTemplate
import org.wfanet.measurement.internal.kingdom.StreamReportsRequest
import org.wfanet.measurement.internal.kingdom.UpdateReportStateRequest
import org.wfanet.measurement.service.internal.kingdom.testing.FakeReportConfigScheduleStorage
import org.wfanet.measurement.service.internal.kingdom.testing.FakeReportConfigStorage
import org.wfanet.measurement.service.internal.kingdom.testing.FakeReportStorage
import org.wfanet.measurement.service.internal.kingdom.testing.FakeRequisitionStorage
import org.wfanet.measurement.service.testing.GrpcTestServerRule

class ReportStarterClientImplTest {

  private val fakeReportConfigStorage = FakeReportConfigStorage()
  private val fakeReportConfigScheduleStorage = FakeReportConfigScheduleStorage()
  private val fakeReportStorage = FakeReportStorage()
  private val fakeRequisitionStorage = FakeRequisitionStorage()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    listOf(
      fakeReportConfigStorage,
      fakeReportConfigScheduleStorage,
      fakeReportStorage,
      fakeRequisitionStorage
    )
  }

  private val reportStarterClient: ReportStarterClient by lazy {
    val channel = grpcTestServerRule.channel
    ReportStarterClientImpl(
      ReportConfigStorageCoroutineStub(channel),
      ReportConfigScheduleStorageCoroutineStub(channel),
      ReportStorageCoroutineStub(channel),
      RequisitionStorageCoroutineStub(channel)
    )
  }

  @Test
  fun createNextReport() = runBlocking<Unit> {
    fakeReportStorage.mocker.mock(FakeReportStorage::createNextReport) {
      Report.getDefaultInstance()
    }

    val schedule =
      ReportConfigSchedule.newBuilder()
        .setExternalScheduleId(12345)
        .build()

    reportStarterClient.createNextReport(schedule)

    val expectedRequest =
      CreateNextReportRequest.newBuilder()
        .setExternalScheduleId(12345)
        .build()

    assertThat(fakeReportStorage.mocker.callsForMethod("createNextReport"))
      .containsExactly(expectedRequest)
  }

  @Test
  fun buildRequisitionsForReport() = runBlocking<Unit> {
    val requisitionTemplate = RequisitionTemplate.newBuilder().apply {
      externalDataProviderId = 1
      externalCampaignId = 2
      requisitionDetailsBuilder.metricDefinitionBuilder.sketchBuilder.sketchConfigId = 3
    }.build()

    fakeReportConfigStorage.mocker.mock(FakeReportConfigStorage::listRequisitionTemplates) {
      ListRequisitionTemplatesResponse.newBuilder()
        .addRequisitionTemplates(requisitionTemplate)
        .build()
    }

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

    assertThat(reportStarterClient.buildRequisitionsForReport(report))
      .containsExactly(expectedRequisition)

    val expectedRequest =
      ListRequisitionTemplatesRequest.newBuilder()
        .setExternalReportConfigId(4)
        .build()

    assertThat(fakeReportConfigStorage.mocker.callsForMethod("listRequisitionTemplates"))
      .containsExactly(expectedRequest)
  }

  @Test
  fun createRequisition() = runBlocking<Unit> {
    val inputRequisition = Requisition.newBuilder().setExternalDataProviderId(1).build()
    val outputRequisition = Requisition.newBuilder().setExternalDataProviderId(2).build()

    fakeRequisitionStorage.mocker.mock(FakeRequisitionStorage::createRequisition) {
      outputRequisition
    }

    assertThat(reportStarterClient.createRequisition(inputRequisition))
      .isEqualTo(outputRequisition)

    assertThat(fakeRequisitionStorage.mocker.callsForMethod("createRequisition"))
      .containsExactly(inputRequisition)
  }

  @Test
  fun associateRequisitionToReport() = runBlocking<Unit> {
    fakeReportStorage.mocker.mock(FakeReportStorage::associateRequisition) {
      AssociateRequisitionResponse.getDefaultInstance()
    }

    val requisition = Requisition.newBuilder().setExternalRequisitionId(1).build()
    val report = Report.newBuilder().setExternalReportId(2).build()

    reportStarterClient.associateRequisitionToReport(requisition, report)

    val expectedRequest = AssociateRequisitionRequest.newBuilder().apply {
      externalRequisitionId = 1
      externalReportId = 2
    }.build()

    assertThat(fakeReportStorage.mocker.callsForMethod("associateRequisition"))
      .containsExactly(expectedRequest)
  }

  @Test
  fun updateReportState() = runBlocking<Unit> {
    val outputReport = Report.getDefaultInstance()
    fakeReportStorage.mocker.mock(FakeReportStorage::updateReportState) { outputReport }

    val report = Report.newBuilder().setExternalReportId(1).build()
    val newState = ReportState.IN_PROGRESS

    reportStarterClient.updateReportState(report, newState)

    val expectedRequest =
      UpdateReportStateRequest.newBuilder()
        .setExternalReportId(report.externalReportId)
        .setState(newState)
        .build()

    assertThat(fakeReportStorage.mocker.callsForMethod("updateReportState"))
      .containsExactly(expectedRequest)
  }

  @Test
  fun streamReportsInState() = runBlocking<Unit> {
    val report1 = Report.newBuilder().setExternalReportId(1).build()
    val report2 = Report.newBuilder().setExternalReportId(2).build()
    val report3 = Report.newBuilder().setExternalReportId(3).build()

    fakeReportStorage.mocker.mockStreaming(FakeReportStorage::streamReports) {
      flowOf(report1, report2, report3)
    }

    val state = ReportState.AWAITING_REQUISITION_FULFILLMENT
    val outputReports = reportStarterClient.streamReportsInState(state)

    assertThat(outputReports.toList())
      .containsExactly(report1, report2, report3)
      .inOrder()

    val expectedRequest = StreamReportsRequest.newBuilder().apply {
      filterBuilder.addStates(state)
    }.build()

    assertThat(fakeReportStorage.mocker.callsForMethod("streamReports"))
      .comparingExpectedFieldsOnly()
      .containsExactly(expectedRequest)
  }

  @Test
  fun streamReadyReports() = runBlocking<Unit> {
    val report1 = Report.newBuilder().setExternalReportId(1).build()
    val report2 = Report.newBuilder().setExternalReportId(2).build()
    val report3 = Report.newBuilder().setExternalReportId(3).build()

    fakeReportStorage.mocker.mockStreaming(FakeReportStorage::streamReadyReports) {
      flowOf(report1, report2, report3)
    }

    val outputReports = reportStarterClient.streamReadyReports()

    assertThat(outputReports.toList())
      .containsExactly(report1, report2, report3)
      .inOrder()
  }

  @Test
  fun streamReadySchedules() = runBlocking<Unit> {
    val schedule1 = ReportConfigSchedule.newBuilder().setExternalScheduleId(1).build()
    val schedule2 = ReportConfigSchedule.newBuilder().setExternalScheduleId(2).build()
    val schedule3 = ReportConfigSchedule.newBuilder().setExternalScheduleId(3).build()

    fakeReportConfigScheduleStorage.mocker.mockStreaming(
      FakeReportConfigScheduleStorage::streamReadyReportConfigSchedules
    ) {
      flowOf(schedule1, schedule2, schedule3)
    }

    val outputSchedules = reportStarterClient.streamReadySchedules()

    assertThat(outputSchedules.toList())
      .containsExactly(schedule1, schedule2, schedule3)
  }
}
