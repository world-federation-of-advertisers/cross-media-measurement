// Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.bff.service.api.v1alpha

import io.grpc.Status
import java.util.logging.Logger
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.reporting.bff.v1alpha.GetReportRequest
import org.wfanet.measurement.reporting.bff.v1alpha.ListReportsRequest
import org.wfanet.measurement.reporting.bff.v1alpha.ListReportsResponse
import org.wfanet.measurement.reporting.bff.v1alpha.Report
import org.wfanet.measurement.reporting.bff.v1alpha.ReportView
import org.wfanet.measurement.reporting.bff.v1alpha.ReportsGrpcKt
import org.wfanet.measurement.reporting.bff.v1alpha.listReportsResponse
import org.wfanet.measurement.reporting.bff.v1alpha.report
import org.wfanet.measurement.reporting.v2alpha.Report as BackendReport
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt as BackendReportsGrpcKt
import org.wfanet.measurement.reporting.v2alpha.getReportRequest
import org.wfanet.measurement.reporting.v2alpha.listReportsRequest

class ReportsService(private val backendReportsStub: BackendReportsGrpcKt.ReportsCoroutineStub) :
  ReportsGrpcKt.ReportsCoroutineImplBase() {
  override suspend fun listReports(request: ListReportsRequest): ListReportsResponse {
    // TODO(@bdomen-ggl): Still working on UX for pagination, so holding off for now.
    // Will hold off on internally looping the request until it becomes an issue (eg. no reports
    // returned)
    if (request.pageSize != 0 || request.pageToken.isNotEmpty()) {
      throw Status.UNIMPLEMENTED.withDescription("PageSize and PageToken not implemented yet")
        .asRuntimeException()
    }

    val backendRequest = listReportsRequest {
      parent = request.parent
      pageSize = 1000
    }

    val resp = runBlocking(Dispatchers.IO) { backendReportsStub.listReports(backendRequest) }

    if (resp.nextPageToken.isNotEmpty()) {
      logger.warning { "Additional ListReport items. Not Loopping through additional pages." }
    }

    val view =
      if (request.view == ReportView.REPORT_VIEW_UNSPECIFIED) ReportView.REPORT_VIEW_BASIC
      else request.view
    val results = listReportsResponse {
      resp.reportsList
        .filter { it.tags.containsKey("ui.halo-cmm.org") }
        .forEach { reports += it.toBffReport(view) }
    }
    return results
  }

  override suspend fun getReport(request: GetReportRequest): Report {
    val backendRequest = getReportRequest { name = request.name }

    val resp = runBlocking(Dispatchers.IO) { backendReportsStub.getReport(backendRequest) }

    val view =
      if (request.view == ReportView.REPORT_VIEW_UNSPECIFIED) ReportView.REPORT_VIEW_FULL
      else request.view
    val result = resp.toBffReport(view)
    return result
  }

  private fun BackendReport.toBffReport(view: ReportView): Report {
    val source = this

    return when (view) {
      ReportView.REPORT_VIEW_BASIC -> source.toBasicReport()
      ReportView.REPORT_VIEW_FULL -> source.toFullReport()
      else ->
        throw Status.INVALID_ARGUMENT.withDescription("View type must be specified")
          .asRuntimeException()
    }
  }

  private fun BackendReport.toBasicReport(): Report {
    val source = this

    return report {
      reportId = source.name
      name = source.name
      state = source.state.toBffState()
    }
  }

  private fun BackendReport.toFullReport(): Report {
    val source = this

    // TODO(@bdomen-ggl): Currently stubbed, working to do proper translations in follow up PR
    //   after the proto definitions for the BFF have been updated.
    return report { name = source.name }
  }

  private fun BackendReport.State.toBffState(): Report.State {
    val source = this
    return when (source) {
      BackendReport.State.STATE_UNSPECIFIED -> Report.State.STATE_UNSPECIFIED
      BackendReport.State.RUNNING -> Report.State.RUNNING
      BackendReport.State.SUCCEEDED -> Report.State.SUCCEEDED
      BackendReport.State.FAILED -> Report.State.FAILED
      else -> Report.State.STATE_UNSPECIFIED
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
