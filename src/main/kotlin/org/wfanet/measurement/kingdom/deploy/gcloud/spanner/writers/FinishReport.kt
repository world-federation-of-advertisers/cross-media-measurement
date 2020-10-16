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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Value
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.gcloud.spanner.bufferTo
import org.wfanet.measurement.gcloud.spanner.toProtoBytes
import org.wfanet.measurement.gcloud.spanner.toProtoEnum
import org.wfanet.measurement.gcloud.spanner.toProtoJson
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.ReportDetails
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ReportReader

class FinishReport(
  private val externalReportId: ExternalId,
  private val result: ReportDetails.Result
) : SpannerWriter<Report, Report>() {
  override suspend fun TransactionScope.runTransaction(): Report {
    val reportReadResult = ReportReader().readExternalId(transactionContext, externalReportId)
    require(reportReadResult.report.state == ReportState.IN_PROGRESS) {
      "Report can't be finished because it is not IN_PROGRESS: ${reportReadResult.report}"
    }

    val newReportDetails =
      reportReadResult.report.reportDetails.toBuilder()
        .setResult(result)
        .build()

    Mutation.newUpdateBuilder("Reports")
      .set("AdvertiserId").to(reportReadResult.advertiserId)
      .set("ReportConfigId").to(reportReadResult.reportConfigId)
      .set("ScheduleId").to(reportReadResult.scheduleId)
      .set("ReportId").to(reportReadResult.reportId)
      .set("State").toProtoEnum(ReportState.SUCCEEDED)
      .set("ReportDetails").toProtoBytes(newReportDetails)
      .set("ReportDetailsJson").toProtoJson(newReportDetails)
      .set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
      .build()
      .bufferTo(transactionContext)

    return reportReadResult.report.toBuilder().apply {
      state = ReportState.SUCCEEDED
      reportDetails = newReportDetails
      reportDetailsJson = newReportDetails.toJson()
    }.build()
  }

  override fun ResultScope<Report>.buildResult(): Report {
    return checkNotNull(transactionResult).toBuilder().apply {
      updateTime = commitTimestamp.toProto()
    }.build()
  }
}
