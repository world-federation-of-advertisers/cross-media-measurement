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
import org.wfanet.measurement.gcloud.spanner.getProtoEnum
import org.wfanet.measurement.gcloud.spanner.getProtoMessage
import org.wfanet.measurement.internal.kingdom.ReportConfig
import org.wfanet.measurement.internal.kingdom.ReportConfig.ReportConfigState
import org.wfanet.measurement.internal.kingdom.ReportConfigDetails

class ReportConfigReader : SpannerReader<ReportConfigReader.Result>() {
  data class Result(
    val reportConfig: ReportConfig,
    val advertiserId: Long,
    val reportConfigId: Long
  )

  override val baseSql: String =
    """
    SELECT
      Advertisers.ExternalAdvertiserId,
      ReportConfigs.AdvertiserId,
      ReportConfigs.ReportConfigId,
      ReportConfigs.ExternalReportConfigId,
      ReportConfigs.NumRequisitions,
      ReportConfigs.State,
      ReportConfigs.ReportConfigDetails,
      ReportConfigs.ReportConfigDetailsJson
    FROM ReportConfigs
    JOIN Advertisers USING (AdvertiserId)
    """.trimIndent()

  override val externalIdColumn: String = "ReportConfigs.ExternalReportConfigId"

  override suspend fun translate(struct: Struct): Result =
    Result(
      buildReportConfig(struct),
      struct.getLong("AdvertiserId"),
      struct.getLong("ReportConfigId")
    )

  private fun buildReportConfig(struct: Struct): ReportConfig = ReportConfig.newBuilder().apply {
    externalAdvertiserId = struct.getLong("ExternalAdvertiserId")
    externalReportConfigId = struct.getLong("ExternalReportConfigId")
    numRequisitions = struct.getLong("NumRequisitions")
    state = struct.getProtoEnum("State", ReportConfigState::forNumber)
    reportConfigDetails =
      struct.getProtoMessage("ReportConfigDetails", ReportConfigDetails.parser())
    reportConfigDetailsJson = struct.getString("ReportConfigDetailsJson")
  }.build()
}
