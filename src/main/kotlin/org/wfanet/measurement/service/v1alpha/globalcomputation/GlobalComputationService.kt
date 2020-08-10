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

import com.google.protobuf.Timestamp
import java.time.Duration
import java.time.Instant
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import org.wfanet.measurement.api.v1alpha.GetGlobalComputationRequest
import org.wfanet.measurement.api.v1alpha.GlobalComputation
import org.wfanet.measurement.api.v1alpha.GlobalComputation.State
import org.wfanet.measurement.api.v1alpha.GlobalComputationsGrpcKt.GlobalComputationsCoroutineImplBase
import org.wfanet.measurement.api.v1alpha.StreamActiveGlobalComputationsRequest
import org.wfanet.measurement.api.v1alpha.StreamActiveGlobalComputationsResponse
import org.wfanet.measurement.common.ApiId
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.renewedFlow
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.GetReportRequest
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.ReportStorageGrpcKt.ReportStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamReportsRequest

class GlobalComputationService(
  private val reportStorageStub: ReportStorageCoroutineStub
) : GlobalComputationsCoroutineImplBase() {
  override suspend fun getGlobalComputation(
    request: GetGlobalComputationRequest
  ): GlobalComputation {
    val externalReportId = getExternalReportId(request.key.globalComputationId)
    val report = getReport(externalReportId)
    return translateReportToGlobalComputation(report)
  }

  override fun streamActiveGlobalComputations(
    request: StreamActiveGlobalComputationsRequest
  ): Flow<StreamActiveGlobalComputationsResponse> {
    var lastUpdateTime = ContinuationTokenConverter.decode(request.continuationToken)
    return renewedFlow(Duration.ofHours(1), Duration.ofSeconds(10)) {
      streamActiveReports(lastUpdateTime)
        .onEach {
          lastUpdateTime = maxOf(lastUpdateTime, it.updateTime.toInstant())
        }
        .map { report ->
          StreamActiveGlobalComputationsResponse.newBuilder().apply {
            continuationToken = ContinuationTokenConverter.encode(lastUpdateTime)
            // TODO: rename proto field to be singular:
            globalComputation = translateReportToGlobalComputation(report)
          }.build()
        }
    }
  }

  private fun translateReportToGlobalComputation(report: Report): GlobalComputation =
    GlobalComputation.newBuilder().apply {
      keyBuilder.globalComputationId = ExternalId(report.externalReportId).apiId.value
      state = translateState(report.state)
      // TODO: populate more fields once they're added.
    }.build()

  private fun translateState(reportState: ReportState): State =
    when (reportState) {
      ReportState.AWAITING_REQUISITION_CREATION,
      ReportState.AWAITING_REQUISITION_FULFILLMENT -> State.CREATED
      ReportState.AWAITING_DUCHY_CONFIRMATION -> State.CONFIRMING
      ReportState.IN_PROGRESS -> State.RUNNING
      ReportState.SUCCEEDED -> State.SUCCEEDED
      ReportState.FAILED -> State.FAILED
      ReportState.CANCELLED -> State.CANCELLED
      ReportState.REPORT_STATE_UNKNOWN,
      ReportState.UNRECOGNIZED -> State.STATE_UNSPECIFIED
    }

  private enum class StateType { TERMINAL, NONTERMINAL, INVALID }
  private fun getStateType(reportState: ReportState): StateType =
    when (reportState) {
      ReportState.AWAITING_REQUISITION_CREATION,
      ReportState.AWAITING_REQUISITION_FULFILLMENT,
      ReportState.AWAITING_DUCHY_CONFIRMATION,
      ReportState.IN_PROGRESS -> StateType.NONTERMINAL
      ReportState.SUCCEEDED,
      ReportState.FAILED,
      ReportState.CANCELLED -> StateType.TERMINAL
      ReportState.REPORT_STATE_UNKNOWN,
      ReportState.UNRECOGNIZED -> StateType.INVALID
    }

  private suspend fun getReport(externalReportId: ExternalId): Report {
    val request =
      GetReportRequest.newBuilder()
        .setExternalReportId(externalReportId.value)
        .build()

    return reportStorageStub.getReport(request)
  }

  private fun getExternalReportId(globalComputationId: String): ExternalId {
    return ApiId(globalComputationId).externalId
  }

  private fun streamActiveReports(lastUpdateTime: Instant): Flow<Report> {
    val request = StreamReportsRequest.newBuilder().apply {
      filterBuilder.apply {
        updatedAfter = lastUpdateTime.toProtoTime()
        addAllStates(ReportState.values().filter { getStateType(it) == StateType.NONTERMINAL })
      }
    }.build()

    return reportStorageStub.streamReports(request)
  }

  companion object {
    object ContinuationTokenConverter {
      fun encode(time: Instant): String = time.toProtoTime().toByteArray().base64UrlEncode()
      fun decode(token: String): Instant = Timestamp.parseFrom(token.base64UrlDecode()).toInstant()
    }
  }
}
