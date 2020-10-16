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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers

import com.google.cloud.spanner.Struct
import org.wfanet.measurement.gcloud.spanner.getProtoMessage
import org.wfanet.measurement.internal.kingdom.RepetitionSpec
import org.wfanet.measurement.internal.kingdom.ReportConfigDetails
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule

/**
 * Reads [ReportConfigSchedule] protos (and primary key) from Spanner.
 */
class ScheduleReader : SpannerReader<ScheduleReader.Result>() {
  data class Result(
    val schedule: ReportConfigSchedule,
    val advertiserId: Long,
    val reportConfigId: Long,
    val scheduleId: Long,
    val reportConfigDetails: ReportConfigDetails
  )

  override val baseSql: String =
    """
    SELECT Advertisers.ExternalAdvertiserId,
           ReportConfigs.ExternalReportConfigId,
           ReportConfigs.ReportConfigDetails,
           ReportConfigSchedules.ExternalScheduleId,
           ReportConfigSchedules.NextReportStartTime,
           ReportConfigSchedules.RepetitionSpec,
           ReportConfigSchedules.RepetitionSpecJson,
           ReportConfigSchedules.AdvertiserId,
           ReportConfigSchedules.ReportConfigId,
           ReportConfigSchedules.ScheduleId
    FROM ReportConfigSchedules
    JOIN Advertisers USING (AdvertiserId)
    JOIN ReportConfigs USING (AdvertiserId, ReportConfigId)
    """.trimIndent()

  override val externalIdColumn: String = "ReportConfigSchedules.ExternalScheduleId"

  override suspend fun translate(struct: Struct): Result =
    Result(
      schedule = buildSchedule(struct),
      advertiserId = struct.getLong("AdvertiserId"),
      reportConfigId = struct.getLong("ReportConfigId"),
      scheduleId = struct.getLong("ScheduleId"),
      reportConfigDetails =
        struct.getProtoMessage("ReportConfigDetails", ReportConfigDetails.parser())
    )

  private fun buildSchedule(struct: Struct): ReportConfigSchedule =
    ReportConfigSchedule.newBuilder().apply {
      externalAdvertiserId = struct.getLong("ExternalAdvertiserId")
      externalReportConfigId = struct.getLong("ExternalReportConfigId")
      externalScheduleId = struct.getLong("ExternalScheduleId")

      nextReportStartTime = struct.getTimestamp("NextReportStartTime").toProto()
      repetitionSpec = struct.getProtoMessage("RepetitionSpec", RepetitionSpec.parser())
      repetitionSpecJson = struct.getString("RepetitionSpecJson")
    }.build()
}
