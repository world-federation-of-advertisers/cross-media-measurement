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

import java.util.logging.Logger
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.reporting.bff.v1alpha.GetReportRequest
import org.wfanet.measurement.reporting.bff.v1alpha.ListReportsRequest
import org.wfanet.measurement.reporting.bff.v1alpha.ListReportsResponse
import org.wfanet.measurement.reporting.bff.v1alpha.Report
import org.wfanet.measurement.reporting.bff.v1alpha.ReportsGrpcKt
import org.wfanet.measurement.reporting.bff.v1alpha.ReportListItem
import org.wfanet.measurement.reporting.bff.v1alpha.listReportsResponse
import org.wfanet.measurement.reporting.bff.v1alpha.report
import org.wfanet.measurement.reporting.bff.v1alpha.reportListItem
import org.wfanet.measurement.reporting.v2alpha.Report as HaloReport
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt as HaloReportsGrpcKt
import org.wfanet.measurement.reporting.v2alpha.getReportRequest
import org.wfanet.measurement.reporting.v2alpha.listReportsRequest

class ReportsService(private val haloReportsStub: HaloReportsGrpcKt.ReportsCoroutineStub) :
  ReportsGrpcKt.ReportsCoroutineImplBase() {
  @Throws(NotImplementedError::class)
  override suspend fun listReports(request: ListReportsRequest): ListReportsResponse {
    // TODO(@bdomen-ggl): Still working on UX for pagination, so holding off for now.
    //  Will hold off on internally looping the request until it becomes an issue (eg. no reports returned)
    if (request.pageSize == 0 || request.pageToken.length > 0) {
      throw NotImplementedError("PageSize and PageToken not implemented yet");
    }

    val haloRequest = listReportsRequest {
      parent = request.parent
      pageSize = 1000
    }

    val resp = runBlocking(Dispatchers.IO) { haloReportsStub.listReports(haloRequest) }

    if (resp.nextPageToken.length > 0) {
      logger.warning { "Additional ListReport items. Not Loopping through additional pages." }
    }

    val results = listReportsResponse {
      resp.reportsList
        .filter { it.tags.containsKey("ui.halo-cmm.org") }
        .forEach {
          val r = reportListItem {
            id = it.name
            name = it.name
            state = convertHaloStateToBffState(it.state)
          }
          reports += r
        }
    }
    return results
  }

  override suspend fun getReport(request: GetReportRequest): Report {
    val haloRequest = getReportRequest { name = request.name }
    val resp = haloReportsStub.getReport(haloRequest)

    // TODO(@bdomen-ggl): Currently stubbed, working to do proper translations in follow up PR
    //   after the proto definitions for the BFF have been updated.
    val result = report { name = resp.name }
    return result
  }

  private fun convertHaloStateToBffState(
    haloReportState: HaloReport.State
  ): ReportListItem.State =
    when (haloReportState) {
      HaloReport.State.STATE_UNSPECIFIED -> ReportListItem.State.STATE_UNSPECIFIED
      HaloReport.State.RUNNING -> ReportListItem.State.RUNNING
      HaloReport.State.SUCCEEDED -> ReportListItem.State.SUCCEEDED
      HaloReport.State.FAILED -> ReportListItem.State.FAILED
      else -> ReportListItem.State.STATE_UNSPECIFIED
    }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
