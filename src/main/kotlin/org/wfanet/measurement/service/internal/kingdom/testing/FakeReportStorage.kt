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

package org.wfanet.measurement.service.internal.kingdom.testing

import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.testing.ServiceMocker
import org.wfanet.measurement.internal.kingdom.AssociateRequisitionRequest
import org.wfanet.measurement.internal.kingdom.AssociateRequisitionResponse
import org.wfanet.measurement.internal.kingdom.ConfirmDuchyReadinessRequest
import org.wfanet.measurement.internal.kingdom.CreateNextReportRequest
import org.wfanet.measurement.internal.kingdom.GetReportRequest
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.ReportStorageGrpcKt.ReportStorageCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamReadyReportsRequest
import org.wfanet.measurement.internal.kingdom.StreamReportsRequest
import org.wfanet.measurement.internal.kingdom.UpdateReportStateRequest

class FakeReportStorage : ReportStorageCoroutineImplBase() {
  val mocker = ServiceMocker<ReportStorageCoroutineImplBase>()

  override suspend fun getReport(request: GetReportRequest): Report =
    mocker.handleCall(request)

  override suspend fun associateRequisition(
    request: AssociateRequisitionRequest
  ): AssociateRequisitionResponse =
    mocker.handleCall(request)

  override suspend fun createNextReport(request: CreateNextReportRequest): Report =
    mocker.handleCall(request)

  override fun streamReadyReports(request: StreamReadyReportsRequest): Flow<Report> =
    mocker.handleCall(request)

  override fun streamReports(request: StreamReportsRequest): Flow<Report> =
    mocker.handleCall(request)

  override suspend fun updateReportState(request: UpdateReportStateRequest): Report =
    mocker.handleCall(request)

  override suspend fun confirmDuchyReadiness(
    request: ConfirmDuchyReadinessRequest
  ): Report =
    mocker.handleCall(request)
}
