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

package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.TransactionContext
import com.google.cloud.spanner.Value
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.db.gcp.spannerDispatcher
import org.wfanet.measurement.db.gcp.toProtoEnum
import org.wfanet.measurement.db.kingdom.gcp.readers.ReportReader
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState

class UpdateReportStateTransaction {
  fun execute(
    transactionContext: TransactionContext,
    externalReportId: ExternalId,
    state: ReportState
  ): Report {
    val reportReadResult = runBlocking(spannerDispatcher()) {
      ReportReader().readExternalId(transactionContext, externalReportId)
    }

    if (reportReadResult.report.state == state) {
      return reportReadResult.report
    }

    require(!reportReadResult.report.state.isTerminal) {
      "Report $externalReportId is in a terminal state: ${reportReadResult.report}"
    }

    val mutation: Mutation =
      Mutation.newUpdateBuilder("Reports")
        .set("AdvertiserId").to(reportReadResult.advertiserId)
        .set("ReportConfigId").to(reportReadResult.reportConfigId)
        .set("ScheduleId").to(reportReadResult.scheduleId)
        .set("ReportId").to(reportReadResult.reportId)
        .set("State").toProtoEnum(state)
        .set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
        .build()

    transactionContext.buffer(mutation)

    return reportReadResult.report.toBuilder().setState(state).build()
  }
}

private val ReportState.isTerminal: Boolean
  get() = when (this) {
    ReportState.AWAITING_REQUISITION_CREATION,
    ReportState.AWAITING_REQUISITION_FULFILLMENT,
    ReportState.AWAITING_DUCHY_CONFIRMATION,
    ReportState.IN_PROGRESS -> false

    ReportState.SUCCEEDED,
    ReportState.FAILED,
    ReportState.CANCELLED,
    ReportState.UNRECOGNIZED,
    ReportState.REPORT_STATE_UNKNOWN -> true
  }
