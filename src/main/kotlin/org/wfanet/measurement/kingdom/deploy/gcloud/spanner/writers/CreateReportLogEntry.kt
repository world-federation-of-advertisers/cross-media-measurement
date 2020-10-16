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
import org.wfanet.measurement.gcloud.spanner.bufferTo
import org.wfanet.measurement.gcloud.spanner.toProtoBytes
import org.wfanet.measurement.gcloud.spanner.toProtoJson
import org.wfanet.measurement.internal.kingdom.ReportLogEntry
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ReportReader

class CreateReportLogEntry(
  private val reportLogEntry: ReportLogEntry
) : SpannerWriter<Unit, ReportLogEntry>() {

  override suspend fun TransactionScope.runTransaction() {
    val externalId = ExternalId(reportLogEntry.externalReportId)
    val reportReadResult = ReportReader().readExternalId(transactionContext, externalId)
    reportLogEntry
      .toInsertMutation(reportReadResult)
      .bufferTo(transactionContext)
  }

  override fun ResultScope<Unit>.buildResult(): ReportLogEntry {
    return reportLogEntry.toBuilder().apply {
      createTime = commitTimestamp.toProto()
    }.build()
  }
}

private fun ReportLogEntry.toInsertMutation(reportReadResult: ReportReader.Result): Mutation =
  Mutation.newInsertBuilder("ReportLogEntries")
    .set("AdvertiserId").to(reportReadResult.advertiserId)
    .set("ReportConfigId").to(reportReadResult.reportConfigId)
    .set("ScheduleId").to(reportReadResult.scheduleId)
    .set("ReportId").to(reportReadResult.reportId)
    .set("CreateTime").to(Value.COMMIT_TIMESTAMP)
    .set("ReportLogDetails").toProtoBytes(reportLogDetails)
    .set("ReportLogDetailsJson").toProtoJson(reportLogDetails)
    .build()
