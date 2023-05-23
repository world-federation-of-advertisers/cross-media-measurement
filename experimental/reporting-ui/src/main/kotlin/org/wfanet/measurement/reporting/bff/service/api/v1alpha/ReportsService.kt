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

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.reporting.bff.v1alpha.GetReportRequest
import org.wfanet.measurement.reporting.bff.v1alpha.ListReportsRequest
import org.wfanet.measurement.reporting.bff.v1alpha.ListReportsResponse
import org.wfanet.measurement.reporting.bff.v1alpha.Report
import org.wfanet.measurement.reporting.bff.v1alpha.ReportsGrpcKt
import org.wfanet.measurement.reporting.bff.v1alpha.listReportsResponse
import org.wfanet.measurement.reporting.bff.v1alpha.report
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt as HaloReportsGrpcKt
import org.wfanet.measurement.reporting.v2alpha.getReportRequest
import org.wfanet.measurement.reporting.v2alpha.listReportsRequest

class ReportsService(private val haloReportsStub: HaloReportsGrpcKt.ReportsCoroutineStub) :
  ReportsGrpcKt.ReportsCoroutineImplBase() {
  override suspend fun listReports(request: ListReportsRequest): ListReportsResponse {
    val haloRequest = listReportsRequest {
      parent = request.parent
      pageSize = if (request.pageSize > 0) request.pageSize else 1000
    }

    val resp = runBlocking(Dispatchers.IO) { haloReportsStub.listReports(haloRequest) }

    val results = listReportsResponse {
      nextPageToken = resp.nextPageToken

      resp.reportsList.forEach {
        // TODO(@bdomen-ggl): Currently stubbed, working to do proper translations in follow up PR
        //   after the proto definitions for the BFF have been updated.
        val r = report { name = it.name }
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
}
