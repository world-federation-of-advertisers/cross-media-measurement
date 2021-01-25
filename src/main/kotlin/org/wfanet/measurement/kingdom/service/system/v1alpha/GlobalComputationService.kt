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

package org.wfanet.measurement.kingdom.service.system.v1alpha

import com.google.protobuf.Timestamp
import io.grpc.Status
import java.time.Duration
import java.time.Instant
import java.util.logging.Logger
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.identity.ApiId
import org.wfanet.measurement.common.identity.DuchyIdentity
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.duchyIdentityFromContext
import org.wfanet.measurement.common.renewedFlow
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.ConfirmDuchyReadinessRequest
import org.wfanet.measurement.internal.kingdom.DuchyLogDetails.MpcAlgorithm
import org.wfanet.measurement.internal.kingdom.FinishReportRequest
import org.wfanet.measurement.internal.kingdom.GetReportRequest
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.ReportLogDetails.ErrorDetails.ErrorType
import org.wfanet.measurement.internal.kingdom.ReportLogEntriesGrpcKt.ReportLogEntriesCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReportLogEntry
import org.wfanet.measurement.internal.kingdom.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamReportsRequest
import org.wfanet.measurement.system.v1alpha.ConfirmGlobalComputationRequest
import org.wfanet.measurement.system.v1alpha.CreateGlobalComputationStatusUpdateRequest
import org.wfanet.measurement.system.v1alpha.FinishGlobalComputationRequest
import org.wfanet.measurement.system.v1alpha.GetGlobalComputationRequest
import org.wfanet.measurement.system.v1alpha.GlobalComputation
import org.wfanet.measurement.system.v1alpha.GlobalComputation.State
import org.wfanet.measurement.system.v1alpha.GlobalComputationStatusUpdate
import org.wfanet.measurement.system.v1alpha.GlobalComputationStatusUpdate.ErrorDetails.ErrorType as ApiErrorType
import org.wfanet.measurement.system.v1alpha.GlobalComputationStatusUpdate.MpcAlgorithm as ApiMpcAlgorithm
import org.wfanet.measurement.system.v1alpha.GlobalComputationsGrpcKt.GlobalComputationsCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.StreamActiveGlobalComputationsRequest
import org.wfanet.measurement.system.v1alpha.StreamActiveGlobalComputationsResponse

class GlobalComputationService(
  private val reportsStub: ReportsCoroutineStub,
  private val reportLogEntriesStub: ReportLogEntriesCoroutineStub,
  private val duchyIdentityProvider: () -> DuchyIdentity = ::duchyIdentityFromContext
) : GlobalComputationsCoroutineImplBase() {
  override suspend fun getGlobalComputation(
    request: GetGlobalComputationRequest
  ): GlobalComputation {
    val externalReportId = getExternalReportId(request.key.globalComputationId)
    val report = getReport(externalReportId)
    return report.toGlobalComputation()
  }

  override fun streamActiveGlobalComputations(
    request: StreamActiveGlobalComputationsRequest
  ): Flow<StreamActiveGlobalComputationsResponse> {
    var lastUpdateTime = ContinuationTokenConverter.decode(request.continuationToken)
    return renewedFlow(Duration.ofHours(1), Duration.ofSeconds(1)) {
      logger.info("Streaming active global computations since $lastUpdateTime")
      streamActiveReports(lastUpdateTime)
        .onEach {
          lastUpdateTime = maxOf(lastUpdateTime, it.updateTime.toInstant())
        }
        .map { report ->
          StreamActiveGlobalComputationsResponse.newBuilder().apply {
            continuationToken = ContinuationTokenConverter.encode(report.updateTime.toInstant())
            globalComputation = report.toGlobalComputation()
          }.build()
        }
    }
  }

  override suspend fun createGlobalComputationStatusUpdate(
    request: CreateGlobalComputationStatusUpdateRequest
  ): GlobalComputationStatusUpdate {
    val reportLogEntry = ReportLogEntry.newBuilder().apply {
      externalReportId = getExternalReportId(request.parent.globalComputationId).value
      sourceBuilder.duchyBuilder.duchyId = duchyIdentityProvider().id
      reportLogDetailsBuilder.apply {
        duchyLogDetailsBuilder.apply {
          reportedDuchyId = request.statusUpdate.selfReportedIdentifier

          val stageDetails = request.statusUpdate.stageDetails
          algorithm = stageDetails.algorithm.toStorageMpcAlgorithm()
          stageNumber = stageDetails.stageNumber
          stageName = stageDetails.stageName
          stageStart = stageDetails.start
          stageAttemptNumber = stageDetails.attemptNumber
        }

        reportMessage = request.statusUpdate.updateMessage

        if (request.statusUpdate.hasErrorDetails()) {
          val errorDetails = request.statusUpdate.errorDetails
          errorDetailsBuilder.apply {
            errorTime = errorDetails.errorTime
            errorMessage = errorDetails.errorMessage
            stacktrace = "TODO: propagate stack trace"
            errorType = errorDetails.errorType.toStorageErrorType()
          }
        }
      }
    }.build()
    val createdReportLogEntry = reportLogEntriesStub.createReportLogEntry(reportLogEntry)
    return request.statusUpdate.toBuilder().setCreateTime(createdReportLogEntry.createTime).build()
  }

  override suspend fun confirmGlobalComputation(
    request: ConfirmGlobalComputationRequest
  ): GlobalComputation {
    val confirmDuchyReadinessRequest = ConfirmDuchyReadinessRequest.newBuilder().apply {
      externalReportId = ApiId(request.key.globalComputationId).externalId.value
      duchyId = duchyIdentityProvider().id
      addAllExternalRequisitionIds(
        request.readyRequisitionsList.map { ApiId(it.metricRequisitionId).externalId.value }
      )
    }.build()
    val report = reportsStub.confirmDuchyReadiness(confirmDuchyReadinessRequest)
    return report.toGlobalComputation()
  }

  override suspend fun finishGlobalComputation(
    request: FinishGlobalComputationRequest
  ): GlobalComputation {
    val finishReportRequest = FinishReportRequest.newBuilder().apply {
      externalReportId = ApiId(request.key.globalComputationId).externalId.value
      resultBuilder.apply {
        reach = request.result.reach
        putAllFrequency(request.result.frequencyMap)
      }
    }.build()

    val report = reportsStub.finishReport(finishReportRequest)
    return report.toGlobalComputation()
  }

  private suspend fun getReport(externalReportId: ExternalId): Report {
    val request =
      GetReportRequest.newBuilder()
        .setExternalReportId(externalReportId.value)
        .build()

    return reportsStub.getReport(request)
  }

  private fun getExternalReportId(globalComputationId: String): ExternalId {
    return ApiId(globalComputationId).externalId
  }

  private fun streamActiveReports(lastUpdateTime: Instant): Flow<Report> {
    val request = StreamReportsRequest.newBuilder().apply {
      filterBuilder.apply {
        updatedAfter = lastUpdateTime.toProtoTime()
        addAllStates(ReportState.values().filter { getStateType(it) == StateType.INTERMEDIATE })
      }
    }.build()

    return reportsStub.streamReports(request)
  }

  private fun Report.toGlobalComputation(): GlobalComputation {
    return GlobalComputation.newBuilder().apply {
      keyBuilder.globalComputationId = ExternalId(externalReportId).apiId.value
      state = translateState(this@toGlobalComputation.state)
      if (state == State.SUCCEEDED) {
        resultBuilder.apply {
          reach = reportDetails.result.reach
          putAllFrequency(reportDetails.result.frequencyMap)
        }
      }

      for (requisition in reportDetails.requisitionsList) {
        if (requisition.duchyId == duchyIdentityProvider().id) {
          addMetricRequisitionsBuilder().apply {
            dataProviderId = ExternalId(requisition.externalDataProviderId).apiId.value
            campaignId = ExternalId(requisition.externalCampaignId).apiId.value
            metricRequisitionId = ExternalId(requisition.externalRequisitionId).apiId.value
          }
        }
      }
    }.build()
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}

private object ContinuationTokenConverter {
  fun encode(time: Instant): String = time.toProtoTime().toByteArray().base64UrlEncode()
  fun decode(token: String): Instant = Timestamp.parseFrom(token.base64UrlDecode()).toInstant()
}

private fun translateState(reportState: ReportState): State =
  when (reportState) {
    ReportState.AWAITING_REQUISITION_CREATION -> State.CREATED
    ReportState.AWAITING_DUCHY_CONFIRMATION -> State.CONFIRMING
    ReportState.IN_PROGRESS -> State.RUNNING
    ReportState.SUCCEEDED -> State.SUCCEEDED
    ReportState.FAILED -> State.FAILED
    ReportState.CANCELLED -> State.CANCELLED
    ReportState.REPORT_STATE_UNKNOWN,
    ReportState.UNRECOGNIZED -> State.STATE_UNSPECIFIED
  }

private enum class StateType { TERMINAL, INTERMEDIATE, INVALID }
private fun getStateType(reportState: ReportState): StateType =
  when (reportState) {
    ReportState.AWAITING_REQUISITION_CREATION,
    ReportState.AWAITING_DUCHY_CONFIRMATION,
    ReportState.IN_PROGRESS -> StateType.INTERMEDIATE
    ReportState.SUCCEEDED,
    ReportState.FAILED,
    ReportState.CANCELLED -> StateType.TERMINAL
    ReportState.REPORT_STATE_UNKNOWN,
    ReportState.UNRECOGNIZED -> StateType.INVALID
  }

private fun ApiMpcAlgorithm.toStorageMpcAlgorithm(): MpcAlgorithm =
  when (this) {
    ApiMpcAlgorithm.LIQUID_LEGIONS_V1 -> MpcAlgorithm.LIQUID_LEGIONS_V1
    ApiMpcAlgorithm.LIQUID_LEGIONS_V2 -> MpcAlgorithm.LIQUID_LEGIONS_V2
    ApiMpcAlgorithm.UNRECOGNIZED,
    ApiMpcAlgorithm.MPC_ALGORITHM_UNKNOWN ->
      throw Status.INVALID_ARGUMENT
        .withDescription("Invalid algorithm: $this")
        .asException()
  }

private fun ApiErrorType.toStorageErrorType(): ErrorType =
  when (this) {
    ApiErrorType.TRANSIENT -> ErrorType.TRANSIENT
    ApiErrorType.PERMANENT -> ErrorType.PERMANENT
    ApiErrorType.ERROR_TYPE_UNKNOWN,
    ApiErrorType.UNRECOGNIZED ->
      throw Status.INVALID_ARGUMENT
        .withDescription("Invalid error_type: $this")
        .asException()
  }
