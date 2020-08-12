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
import java.time.Instant
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v1alpha.CreateGlobalComputationStatusUpdateRequest
import org.wfanet.measurement.api.v1alpha.GetGlobalComputationRequest
import org.wfanet.measurement.api.v1alpha.GlobalComputation
import org.wfanet.measurement.api.v1alpha.GlobalComputation.State
import org.wfanet.measurement.api.v1alpha.GlobalComputationStatusUpdate
import org.wfanet.measurement.api.v1alpha.GlobalComputationsGrpcKt.GlobalComputationsCoroutineStub
import org.wfanet.measurement.api.v1alpha.StreamActiveGlobalComputationsRequest
import org.wfanet.measurement.api.v1alpha.StreamActiveGlobalComputationsResponse
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.DuchyLogDetails
import org.wfanet.measurement.internal.kingdom.GetReportRequest
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.ReportLogDetails
import org.wfanet.measurement.internal.kingdom.ReportLogEntry
import org.wfanet.measurement.internal.kingdom.ReportLogEntryStorageGrpcKt.ReportLogEntryStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReportStorageGrpcKt.ReportStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamReportsRequest
import org.wfanet.measurement.service.internal.kingdom.testing.FakeReportLogEntryStorage
import org.wfanet.measurement.service.internal.kingdom.testing.FakeReportStorage
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
  private val reportStorage = FakeReportStorage()
  private val reportLogEntryStorage = FakeReportLogEntryStorage()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule { channel ->
    listOf(
      reportStorage,
      reportLogEntryStorage,
      GlobalComputationService(
        ReportStorageCoroutineStub(channel),
        ReportLogEntryStorageCoroutineStub(channel),
        DUCHY_AUTH_PROVIDER
      )
    )
  }

  private val stub by lazy { GlobalComputationsCoroutineStub(grpcTestServerRule.channel) }

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

      reportStorage.mocker.mock(FakeReportStorage::getReport) { report }

      assertThat(stub.getGlobalComputation(request))
        .isEqualTo(expectedComputation)

      assertThat(reportStorage.mocker.callsForMethod("getReport"))
        .containsExactly(
          GetReportRequest.newBuilder()
            .setExternalReportId(REPORT.externalReportId)
            .build()
        )

      reportStorage.mocker.reset()
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

    reportStorage.mocker.mockStreaming(FakeReportStorage::streamReports) {
      flowOf(nextReport(), nextReport())
    }

    val requestBuilder = StreamActiveGlobalComputationsRequest.newBuilder()

    val flow = stub.streamActiveGlobalComputations(requestBuilder.build())

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

    assertThat(reportStorage.mocker.callsForMethod("streamReports"))
      .ignoringRepeatedFieldOrder()
      .containsExactly(
        expectedStreamReportsRequest(0),
        expectedStreamReportsRequest(1001),
        expectedStreamReportsRequest(1003)
      )
      .inOrder()
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

    reportLogEntryStorage.mocker.mock(FakeReportLogEntryStorage::createReportLogEntry) {
      it.toBuilder().setCreateTime(Instant.ofEpochSecond(6666).toProtoTime()).build()
    }

    assertThat(stub.createGlobalComputationStatusUpdate(request))
      .isEqualTo(expectedResult)

    assertThat(reportLogEntryStorage.mocker.callsForMethod("createReportLogEntry"))
      .containsExactly(expectedReportLogEntry)
  }
}
