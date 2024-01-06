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

import org.wfanet.measurement.reporting.v2alpha.listReportingSetsRequest
import org.wfanet.measurement.reporting.v2alpha.ReportingSetsGrpcKt
import org.wfanet.measurement.reporting.v2alpha.ReportingSet

class ListReportingSetsRequest(
    val parent: String,
    val pageSize: Int,
){}

class ReportingSetsService(private val backendReportingSetsStub: ReportingSetsGrpcKt.ReportingSetsCoroutineStub) {
  suspend fun ListReportingSets(request: ListReportingSetsRequest): List<ReportingSet> {
    val backendRequest = listReportingSetsRequest {
      parent = request.parent
      pageSize = 1000
    }

    val resp = backendReportingSetsStub.listReportingSets(backendRequest)

    return resp.reportingSetsList
  }
}