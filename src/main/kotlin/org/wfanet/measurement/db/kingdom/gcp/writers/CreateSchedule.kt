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

package org.wfanet.measurement.db.kingdom.gcp.writers

import com.google.cloud.spanner.Mutation
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.InternalId
import org.wfanet.measurement.db.gcp.bufferTo
import org.wfanet.measurement.db.gcp.toProtoBytes
import org.wfanet.measurement.db.gcp.toProtoJson
import org.wfanet.measurement.db.kingdom.gcp.readers.ReportConfigReader
import org.wfanet.measurement.gcloud.toGcloudTimestamp
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule

class CreateSchedule(
  private val schedule: ReportConfigSchedule
) : SpannerWriter<ReportConfigSchedule, ReportConfigSchedule>() {

  override suspend fun TransactionScope.runTransaction(): ReportConfigSchedule {
    val reportConfigReadResult =
      ReportConfigReader()
        .readExternalId(transactionContext, ExternalId(schedule.externalReportConfigId))

    val actualSchedule = schedule.toBuilder().apply {
      externalScheduleId = idGenerator.generateExternalId().value
      nextReportStartTime = repetitionSpec.start
    }.build()

    actualSchedule
      .toMutation(reportConfigReadResult, idGenerator.generateInternalId())
      .bufferTo(transactionContext)

    return actualSchedule
  }

  override fun ResultScope<ReportConfigSchedule>.buildResult(): ReportConfigSchedule {
    return checkNotNull(transactionResult)
  }
}

private fun ReportConfigSchedule.toMutation(
  reportConfigReadResult: ReportConfigReader.Result,
  scheduleId: InternalId
): Mutation {
  return Mutation.newInsertBuilder("ReportConfigSchedules")
    .set("AdvertiserId").to(reportConfigReadResult.advertiserId)
    .set("ReportConfigId").to(reportConfigReadResult.reportConfigId)
    .set("ScheduleId").to(scheduleId.value)
    .set("ExternalScheduleId").to(externalScheduleId)
    .set("NextReportStartTime").to(nextReportStartTime.toGcloudTimestamp())
    .set("RepetitionSpec").toProtoBytes(repetitionSpec)
    .set("RepetitionSpecJson").toProtoJson(repetitionSpec)
    .build()
}
