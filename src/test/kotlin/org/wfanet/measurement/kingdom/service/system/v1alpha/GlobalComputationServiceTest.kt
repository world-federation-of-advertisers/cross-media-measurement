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

package org.wfanet.measurement.kingdom.service.system.v1alpha

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.nhaarman.mockitokotlin2.UseConstructor
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.argumentCaptor
import com.nhaarman.mockitokotlin2.inOrder
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.times
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
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.identity.DuchyIdentity
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.testing.DuchyIdSetter
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.ConfirmDuchyReadinessRequest
import org.wfanet.measurement.internal.kingdom.DuchyLogDetails
import org.wfanet.measurement.internal.kingdom.FinishReportRequest
import org.wfanet.measurement.internal.kingdom.GetReportRequest
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.ReportLogDetails
import org.wfanet.measurement.internal.kingdom.ReportLogEntriesGrpcKt.ReportLogEntriesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ReportLogEntriesGrpcKt.ReportLogEntriesCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReportLogEntry
import org.wfanet.measurement.internal.kingdom.ReportsGrpcKt.ReportsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamReportsRequest
import org.wfanet.measurement.system.v1alpha.ConfirmGlobalComputationRequest
import org.wfanet.measurement.system.v1alpha.CreateGlobalComputationStatusUpdateRequest
import org.wfanet.measurement.system.v1alpha.FinishGlobalComputationRequest
import org.wfanet.measurement.system.v1alpha.GetGlobalComputationRequest
import org.wfanet.measurement.system.v1alpha.GlobalComputation
import org.wfanet.measurement.system.v1alpha.GlobalComputation.State
import org.wfanet.measurement.system.v1alpha.GlobalComputationStatusUpdate
import org.wfanet.measurement.system.v1alpha.StreamActiveGlobalComputationsRequest
import org.wfanet.measurement.system.v1alpha.StreamActiveGlobalComputationsResponse

private const val DUCHY_ID = "some-duchy-id"
private const val OTHER_DUCHY_ID = "other-duchy-id"
private val DUCHY_AUTH_PROVIDER = { DuchyIdentity(DUCHY_ID) }

private val REPORT: Report = Report.newBuilder().apply {
  externalAdvertiserId = 1
  externalReportConfigId = 2
  externalScheduleId = 3
  externalReportId = 4

  reportDetailsBuilder.apply {
    addRequisitionsBuilder().apply {
      externalDataProviderId = 5
      externalCampaignId = 6
      externalRequisitionId = 7
      duchyId = DUCHY_ID
    }
    addRequisitionsBuilder().apply {
      externalDataProviderId = 8
      externalCampaignId = 9
      externalRequisitionId = 10
      duchyId = OTHER_DUCHY_ID
    }
  }
}.build()

private val GLOBAL_COMPUTATION: GlobalComputation = GlobalComputation.newBuilder().apply {
  keyBuilder.globalComputationId = ExternalId(REPORT.externalReportId).apiId.value
  addMetricRequisitionsBuilder().apply {
    dataProviderId = ExternalId(5).apiId.value
    campaignId = ExternalId(6).apiId.value
    metricRequisitionId = ExternalId(7).apiId.value
  }
}.build()

@RunWith(JUnit4::class)
class GlobalComputationServiceTest {
  @get:Rule
  val duchyIdSetter = DuchyIdSetter(DUCHY_ID)

  private val reportStorage: ReportsCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())
  private val reportLogEntryStorage: ReportLogEntriesCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(reportStorage)
    addService(reportLogEntryStorage)
  }

  private val channel = grpcTestServerRule.channel

  private val service =
    GlobalComputationService(
      ReportsCoroutineStub(channel),
      ReportLogEntriesCoroutineStub(channel),
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
        .comparingExpectedFieldsOnly()
        .isEqualTo(expectedComputation)

      verifyProtoArgument(reportStorage, ReportsCoroutineImplBase::getReport)
        .isEqualTo(
          GetReportRequest.newBuilder()
            .setExternalReportId(REPORT.externalReportId)
            .build()
        )

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
          algorithm = GlobalComputationStatusUpdate.MpcAlgorithm.LIQUID_LEGIONS_V1
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
          algorithm = DuchyLogDetails.MpcAlgorithm.LIQUID_LEGIONS_V1
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

    verifyProtoArgument(
      reportLogEntryStorage,
      ReportLogEntriesCoroutineImplBase::createReportLogEntry
    )
      .isEqualTo(expectedReportLogEntry)
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

    verifyProtoArgument(reportStorage, ReportsCoroutineImplBase::confirmDuchyReadiness)
      .isEqualTo(expectedConfirmDuchyReadinessRequest)
  }

  @Test
  fun finishGlobalComputation() = runBlocking<Unit> {
    val request = FinishGlobalComputationRequest.newBuilder().apply {
      keyBuilder.globalComputationId = ExternalId(123).apiId.value
      resultBuilder.apply {
        reach = 456
        putFrequency(1, 0.2)
        putFrequency(3, 0.8)
      }
    }.build()

    whenever(reportStorage.finishReport(any()))
      .thenReturn(
        REPORT.toBuilder().apply {
          state = ReportState.SUCCEEDED
          reportDetailsBuilder.resultBuilder.apply {
            reach = 456
            putFrequency(1, 0.2)
            putFrequency(3, 0.8)
          }
        }.build()
      )

    assertThat(service.finishGlobalComputation(request))
      .isEqualTo(
        GLOBAL_COMPUTATION.toBuilder().apply {
          state = State.SUCCEEDED
          resultBuilder.apply {
            reach = 456
            putFrequency(1, 0.2)
            putFrequency(3, 0.8)
          }
        }.build()
      )

    val expectedFinishReportRequest = FinishReportRequest.newBuilder().apply {
      externalReportId = 123
      resultBuilder.apply {
        reach = 456
        putFrequency(1, 0.2)
        putFrequency(3, 0.8)
      }
    }.build()

    verifyProtoArgument(reportStorage, ReportsCoroutineImplBase::finishReport)
      .isEqualTo(expectedFinishReportRequest)
  }
}
