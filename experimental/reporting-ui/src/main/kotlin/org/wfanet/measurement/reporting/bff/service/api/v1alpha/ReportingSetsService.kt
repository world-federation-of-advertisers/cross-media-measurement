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
import org.wfanet.measurement.reporting.v2alpha.reportingSet

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

    val mockReportingSets = listOf(
      reportingSet {
        name = "measurementConsumers/VCTqwV_vFXw/reportingSets/edp1-2"
        displayName = "EDP1"
        tags.put("ui.halo-cmm.org/reporting_set_type", "individual")
        tags.put("ui.halo-cmm.org/reporting_set_id", "measurementConsumers/VCTqwV_vFXw/reportingSets/edp1-2")
      },
      reportingSet {
        name = "measurementConsumers/VCTqwV_vFXw/reportingSets/edp2-2"
        displayName = "EDP2"
        tags.put("ui.halo-cmm.org/reporting_set_type", "individual")
        tags.put("ui.halo-cmm.org/reporting_set_id", "measurementConsumers/VCTqwV_vFXw/reportingSets/edp2-2")
      },
      reportingSet {
        name = "measurementConsumers/VCTqwV_vFXw/reportingSets/edp3-2"
        displayName = "EDP3"
        tags.put("ui.halo-cmm.org/reporting_set_type", "individual")
        tags.put("ui.halo-cmm.org/reporting_set_id", "measurementConsumers/VCTqwV_vFXw/reportingSets/edp3-2")
      },
      reportingSet {
        name = "measurementConsumers/VCTqwV_vFXw/reportingSets/union-2-edp123"
        displayName = "All EDPs"
        tags.put("ui.halo-cmm.org/reporting_set_type", "union")
      },
      reportingSet {
        name = "measurementConsumers/VCTqwV_vFXw/reportingSets/edp1-2-unique"
        displayName = "EDP1"
        tags.put("ui.halo-cmm.org/reporting_set_type", "unique")
        tags.put("ui.halo-cmm.org/reporting_set_id", "measurementConsumers/VCTqwV_vFXw/reportingSets/edp1-2")
      },
      reportingSet {
        name = "measurementConsumers/VCTqwV_vFXw/reportingSets/edp2-2-unique"
        displayName = "EDP2"
        tags.put("ui.halo-cmm.org/reporting_set_type", "unique")
        tags.put("ui.halo-cmm.org/reporting_set_id", "measurementConsumers/VCTqwV_vFXw/reportingSets/edp2-2")
      },
      reportingSet {
        name = "measurementConsumers/VCTqwV_vFXw/reportingSets/edp3-2-unique"
        displayName = "EDP3"
        tags.put("ui.halo-cmm.org/reporting_set_type", "unique")
        tags.put("ui.halo-cmm.org/reporting_set_id", "measurementConsumers/VCTqwV_vFXw/reportingSets/edp3-2")
      },
      reportingSet {
        name = "measurementConsumers/VCTqwV_vFXw/reportingSets/union-2-edp123"
        displayName = "All EDPs"
        tags.put("ui.halo-cmm.org/reporting_set_type", "union")
      },
    )

    return mockReportingSets

    // val resp = backendReportingSetsStub.listReportingSets(backendRequest)

    // return resp.reportingSetsList
  }
}