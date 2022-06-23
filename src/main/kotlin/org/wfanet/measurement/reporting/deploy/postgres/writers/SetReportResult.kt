// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.deploy.postgres.writers

import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.internal.reporting.Report
import org.wfanet.measurement.internal.reporting.SetReportResultRequest
import org.wfanet.measurement.internal.reporting.copy
import org.wfanet.measurement.reporting.deploy.postgres.readers.ReportReader
import org.wfanet.measurement.reporting.service.internal.ReportNotFoundException

/**
 * Sets the Report result in the database.
 *
 * Throws the following on [execute]:
 * * [ReportNotFoundException] Report not found.
 */
class SetReportResult(private val request: SetReportResultRequest) : PostgresWriter<Report>() {
  override suspend fun TransactionScope.runTransaction(): Report {
    val reportResult =
      ReportReader()
        .getReportByExternalId(
          transactionContext,
          request.measurementConsumerReferenceId,
          request.externalReportId
        )
    val updatedDetails = reportResult.report.details.copy { result = request.result }
    val statement =
      boundStatement(
        """
      UPDATE Reports
      SET ReportDetails = $1, State = $2
      WHERE MeasurementConsumerReferenceId = $3
      AND ExternalReportId = $4
      """
      ) {
        bind("$1", updatedDetails.toByteArray())
        bind("$2", Report.State.SUCCEEDED.number)
        bind("$3", request.measurementConsumerReferenceId)
        bind("$4", request.externalReportId)
      }

    transactionContext.run { executeStatement(statement) }

    return reportResult.report.copy {
      state = Report.State.SUCCEEDED
      details = updatedDetails
    }
  }
}
