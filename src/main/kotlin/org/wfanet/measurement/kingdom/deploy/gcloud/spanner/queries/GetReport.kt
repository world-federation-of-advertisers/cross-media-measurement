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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ReportReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.SpannerReader

class GetReport(externalReportId: ExternalId) : SpannerQuery<ReportReader.Result, Report>() {
  override val reader: SpannerReader<ReportReader.Result> by lazy {
    ReportReader(index = ReportReader.Index.EXTERNAL_ID)
      .withBuilder {
        appendClause("WHERE Reports.ExternalReportId = @external_report_id")
        bind("external_report_id").to(externalReportId.value)

        // The index ReportsByExternalId will enforce that only a single result is returned, but we
        // explicitly add LIMIT 1 to make it obvious, when looking at debug logs or porting this
        // code to other databases, that the query should return a single result.
        appendClause("LIMIT 1")
      }
  }

  override fun Flow<ReportReader.Result>.transform(): Flow<Report> = map { it.report }
}
