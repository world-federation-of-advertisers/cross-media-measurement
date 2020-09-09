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

package org.wfanet.measurement.db.kingdom.gcp.queries

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.db.gcp.appendClause
import org.wfanet.measurement.db.kingdom.gcp.readers.ReportReader
import org.wfanet.measurement.db.kingdom.gcp.readers.SpannerReader
import org.wfanet.measurement.internal.kingdom.Report

class GetReport(externalReportId: ExternalId) : SpannerQuery<ReportReader.Result, Report>() {
  override val reader: SpannerReader<ReportReader.Result> by lazy {
    ReportReader()
      .withBuilder {
        appendClause("WHERE Reports.ExternalReportId = @external_report_id")
        bind("external_report_id").to(externalReportId.value)
      }
  }

  override fun Flow<ReportReader.Result>.transform(): Flow<Report> = map { it.report }
}
