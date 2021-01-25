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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Value
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.spanner.bufferTo
import org.wfanet.measurement.gcloud.spanner.toProtoEnum
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ReportReader

class UpdateReportState(
  private val externalReportId: ExternalId,
  private val state: ReportState
) : SpannerWriter<Report, Report>() {
  override suspend fun TransactionScope.runTransaction(): Report {
    val reportReadResult = ReportReader().readExternalId(transactionContext, externalReportId)
    return updateReportState(reportReadResult, state)
  }

  override fun ResultScope<Report>.buildResult(): Report {
    return checkNotNull(transactionResult).toBuilder()
      .setUpdateTime(commitTimestamp.toProto())
      .build()
  }
}

internal fun SpannerWriter.TransactionScope.updateReportState(
  reportReadResult: ReportReader.Result,
  state: ReportState
): Report {
  val report = reportReadResult.report
  if (report.state == state) {
    return report
  }

  require(!report.state.isTerminal) {
    "Report ${report.externalReportId} is in a terminal state: $report"
  }

  Mutation.newUpdateBuilder("Reports")
    .set("AdvertiserId").to(reportReadResult.advertiserId)
    .set("ReportConfigId").to(reportReadResult.reportConfigId)
    .set("ScheduleId").to(reportReadResult.scheduleId)
    .set("ReportId").to(reportReadResult.reportId)
    .set("State").toProtoEnum(state)
    .set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
    .build()
    .bufferTo(transactionContext)

  return report.toBuilder().setState(state).build()
}

private val ReportState.isTerminal: Boolean
  get() = when (this) {
    ReportState.AWAITING_REQUISITION_CREATION,
    ReportState.AWAITING_DUCHY_CONFIRMATION,
    ReportState.IN_PROGRESS -> false

    ReportState.SUCCEEDED,
    ReportState.FAILED,
    ReportState.CANCELLED,
    ReportState.UNRECOGNIZED,
    ReportState.REPORT_STATE_UNKNOWN -> true
  }
