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
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.bufferTo
import org.wfanet.measurement.gcloud.spanner.toProtoBytes
import org.wfanet.measurement.gcloud.spanner.toProtoJson
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ReportConfigReader

class CreateSchedule(private val schedule: ReportConfigSchedule) :
  SimpleSpannerWriter<ReportConfigSchedule>() {

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
