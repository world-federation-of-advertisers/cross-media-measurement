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

package org.wfanet.measurement.service.v1alpha.globalcomputation

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.nhaarman.mockitokotlin2.UseConstructor
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.argumentCaptor
import com.nhaarman.mockitokotlin2.inOrder
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.times
import com.nhaarman.mockitokotlin2.verify
import com.nhaarman.mockitokotlin2.whenever
import java.time.Instant
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.Mockito
import org.wfanet.measurement.api.v1alpha.ConfirmGlobalComputationRequest
import org.wfanet.measurement.api.v1alpha.CreateGlobalComputationStatusUpdateRequest
import org.wfanet.measurement.api.v1alpha.GetGlobalComputationRequest
import org.wfanet.measurement.api.v1alpha.GlobalComputation
import org.wfanet.measurement.api.v1alpha.GlobalComputation.State
import org.wfanet.measurement.api.v1alpha.GlobalComputationStatusUpdate
import org.wfanet.measurement.api.v1alpha.StreamActiveGlobalComputationsRequest
import org.wfanet.measurement.api.v1alpha.StreamActiveGlobalComputationsResponse
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.testing.DuchyIdSetter
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.ConfirmDuchyReadinessRequest
import org.wfanet.measurement.internal.kingdom.DuchyLogDetails
import org.wfanet.measurement.internal.kingdom.GetReportRequest
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.ReportLogDetails
import org.wfanet.measurement.internal.kingdom.ReportLogEntry
import org.wfanet.measurement.internal.kingdom.ReportLogEntryStorageGrpcKt.ReportLogEntryStorageCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ReportLogEntryStorageGrpcKt.ReportLogEntryStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReportStorageGrpcKt.ReportStorageCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ReportStorageGrpcKt.ReportStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamReportsRequest
import org.wfanet.measurement.service.testing.GrpcTestServerRule
import org.wfanet.measurement.service.v1alpha.common.DuchyAuth

private val REPORT: Report = Report.newBuilder().apply {
  externalAdvertiserId = 1
  externalReportConfigId = 2
  externalScheduleId = 3
  externalReportId = 4
}.build()

private val GLOBAL_COMPUTATION: GlobalComputation = GlobalComputation.newBuilder().apply {
  keyBuilder.globalComputationId = ExternalId(REPORT.externalReportId).apiId.value
}.build()

private const val DUCHY_ID = "some-duchy-id"
private val DUCHY_AUTH_PROVIDER = { DuchyAuth(DUCHY_ID) }

@RunWith(JUnit4::class)
class GlobalComputationsServiceTest {
  @get:Rule
  val duchyIdSetter = DuchyIdSetter(DUCHY_ID)

  private val reportStorage: ReportStorageCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())
  private val reportLogEntryStorage: ReportLogEntryStorageCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule { listOf(reportStorage, reportLogEntryStorage) }

  private val channel = grpcTestServerRule.channel

  private val service =
    GlobalComputationService(
      ReportStorageCoroutineStub(channel),
      ReportLogEntryStorageCoroutineStub(channel),
      DUCHY_AUTH_PROVIDER
    )

  @Test
  fun getGlobalComputation() = runBlocking<Unit> {
    val request =
      GetGlobalComputationRequest.newBuilder()
        .setKey(GLOBAL_COMPUTATION.key)
        .build()

    val stateMap = mapOf(
      ReportState.AWAITING_REQUISITION_CREATION to State.CREATED,
      ReportState.AWAITING_REQUISITION_FULFILLMENT to State.CREATED,
      ReportState.AWAITING_DUCHY_CONFIRMATION to State.CONFIRMING,
      ReportState.IN_PROGRESS to State.RUNNING,
      ReportState.SUCCEEDED to State.SUCCEEDED,
      ReportState.FAILED to State.FAILED,
      ReportState.CANCELLED to State.CANCELLED
    )

    for ((reportState, computationState) in stateMap) {
      val report = REPORT.toBuilder().setState(reportState).build()
      val expectedComputation = GLOBAL_COMPUTATION.toBuilder().setState(computationState).build()

      whenever(reportStorage.getReport(any())).thenReturn(report)

      assertThat(service.getGlobalComputation(request))
        .isEqualTo(expectedComputation)

      argumentCaptor<GetReportRequest> {
        verify(reportStorage).getReport(capture())
        assertThat(firstValue)
          .isEqualTo(
            GetReportRequest.newBuilder()
              .setExternalReportId(REPORT.externalReportId)
              .build()
          )
      }

      Mockito.reset(reportStorage)
    }
  }

  @Test
  fun streamActiveGlobalComputations() = runBlocking<Unit> {
    var calls = 0L
    fun nextReport() =
      REPORT.toBuilder().apply {
        externalReportId = 100 + calls
        updateTimeBuilder.seconds = 1000 + calls
        calls++
      }.build()

    fun expectedResponse(id: Long) =
      StreamActiveGlobalComputationsResponse.newBuilder().apply {
        globalComputationBuilder.keyBuilder.globalComputationId = ExternalId(id).apiId.value
      }.build()

    whenever(reportStorage.streamReports(any()))
      .thenAnswer {
        flowOf(nextReport(), nextReport())
      }

    val flow = service.streamActiveGlobalComputations(
      StreamActiveGlobalComputationsRequest.getDefaultInstance()
    )

    assertThat(flow.take(5).toList())
      .comparingExpectedFieldsOnly()
      .containsExactly(
        expectedResponse(100),
        expectedResponse(101),
        expectedResponse(102),
        expectedResponse(103),
        expectedResponse(104)
      )
      .inOrder()

    fun expectedStreamReportsRequest(updatedAfterSeconds: Long) =
      StreamReportsRequest.newBuilder().apply {
        filterBuilder.apply {
          addAllStates(
            listOf(
              ReportState.AWAITING_REQUISITION_CREATION,
              ReportState.AWAITING_REQUISITION_FULFILLMENT,
              ReportState.AWAITING_DUCHY_CONFIRMATION,
              ReportState.IN_PROGRESS
            )
          )

          updatedAfterBuilder.seconds = updatedAfterSeconds
        }
      }.build()

    inOrder(reportStorage) {
      argumentCaptor<StreamReportsRequest> {
        verify(reportStorage, times(3)).streamReports(capture())
        assertThat(allValues)
          .ignoringRepeatedFieldOrder()
          .containsExactly(
            expectedStreamReportsRequest(0),
            expectedStreamReportsRequest(1001),
            expectedStreamReportsRequest(1003)
          )
          .inOrder()
      }
    }
  }

  @Test
  fun createGlobalComputationStatusUpdate() = runBlocking<Unit> {
    val request = CreateGlobalComputationStatusUpdateRequest.newBuilder().apply {
      parentBuilder.globalComputationId = ExternalId(1111).apiId.value
      statusUpdateBuilder.apply {
        selfReportedIdentifier = "some-self-reported-duchy-identifier"
        stageDetailsBuilder.apply {
          startBuilder.seconds = 2222
          algorithm = GlobalComputationStatusUpdate.MpcAlgorithm.LIQUID_LEGIONS
          stageNumber = 3333
          stageName = "SOME_STAGE"
          attemptNumber = 4444
        }
        updateMessage = "some-update-message"
        errorDetailsBuilder.apply {
          errorTimeBuilder.seconds = 5555
          errorType = GlobalComputationStatusUpdate.ErrorDetails.ErrorType.TRANSIENT
          errorMessage = "some-error-message"
        }
      }
    }.build()

    val expectedResult =
      request.statusUpdate.toBuilder()
        .setCreateTime(Instant.ofEpochSecond(6666).toProtoTime())
        .build()

    val expectedReportLogEntry = ReportLogEntry.newBuilder().apply {
      externalReportId = 1111
      sourceBuilder.duchyBuilder.duchyId = DUCHY_ID
      reportLogDetailsBuilder.apply {
        duchyLogDetailsBuilder.apply {
          reportedDuchyId = "some-self-reported-duchy-identifier"
          stageStartBuilder.seconds = 2222
          algorithm = DuchyLogDetails.MpcAlgorithm.LIQUID_LEGIONS
          stageNumber = 3333
          stageName = "SOME_STAGE"
          stageAttemptNumber = 4444
        }
        reportMessage = "some-update-message"
        errorDetailsBuilder.apply {
          errorTimeBuilder.seconds = 5555
          errorType = ReportLogDetails.ErrorDetails.ErrorType.TRANSIENT
          errorMessage = "some-error-message"
          stacktrace = "TODO: propagate stack trace"
        }
      }
    }.build()

    whenever(reportLogEntryStorage.createReportLogEntry(any()))
      .thenAnswer {
        it.getArgument<ReportLogEntry>(0)
          .toBuilder()
          .setCreateTime(Instant.ofEpochSecond(6666).toProtoTime())
          .build()
      }

    assertThat(service.createGlobalComputationStatusUpdate(request))
      .isEqualTo(expectedResult)

    argumentCaptor<ReportLogEntry> {
      verify(reportLogEntryStorage).createReportLogEntry(capture())
      assertThat(firstValue)
        .isEqualTo(expectedReportLogEntry)
    }
  }

  @Test
  fun confirmGlobalComputation() = runBlocking<Unit> {
    val request = ConfirmGlobalComputationRequest.newBuilder().apply {
      keyBuilder.globalComputationId = ExternalId(1111).apiId.value
      addReadyRequisitionsBuilder().apply {
        dataProviderId = ExternalId(2222).apiId.value
        campaignId = ExternalId(3333).apiId.value
        metricRequisitionId = ExternalId(4444).apiId.value
      }
      addReadyRequisitionsBuilder().apply {
        dataProviderId = ExternalId(5555).apiId.value
        campaignId = ExternalId(6666).apiId.value
        metricRequisitionId = ExternalId(7777).apiId.value
      }
    }.build()

    whenever(reportStorage.confirmDuchyReadiness(any()))
      .thenReturn(REPORT)

    assertThat(service.confirmGlobalComputation(request))
      .isEqualTo(GLOBAL_COMPUTATION)

    val expectedConfirmDuchyReadinessRequest = ConfirmDuchyReadinessRequest.newBuilder().apply {
      externalReportId = ExternalId(1111).value
      duchyId = DUCHY_ID
      addAllExternalRequisitionIds(listOf(ExternalId(4444).value, ExternalId(7777).value))
    }.build()

    argumentCaptor<ConfirmDuchyReadinessRequest> {
      verify(reportStorage).confirmDuchyReadiness(capture())
      assertThat(firstValue)
        .isEqualTo(expectedConfirmDuchyReadinessRequest)
    }
  }
}
