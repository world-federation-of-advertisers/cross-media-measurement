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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers

import com.google.cloud.spanner.Struct
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.internal.kingdom.Report

/**
 * Reads [Report] protos from Spanner via the ReportRequisitions table.
 */
class ReportRequisitionReader : SpannerReader<ReportReader.Result>() {
  override val baseSql: String =
    """
    SELECT ${ReportReader.SELECT_COLUMNS_SQL}
    FROM ReportRequisitions
    JOIN Advertisers USING (AdvertiserId)
    JOIN ReportConfigs USING (AdvertiserId, ReportConfigId)
    JOIN ReportConfigSchedules USING (AdvertiserId, ReportConfigId, ScheduleId)
    JOIN Reports USING (AdvertiserId, ReportConfigId, ScheduleId, ReportId)
    """.trimIndent()

  override val externalIdColumn: String = "Reports.ExternalReportId"

  override suspend fun translate(struct: Struct): ReportReader.Result {
    return ReportReader().translate(struct)
  }

  companion object {
    fun readReportsWithAssociatedRequisition(
      readContext: AsyncDatabaseClient.ReadContext,
      dataProviderId: InternalId,
      campaignId: InternalId,
      requisitionId: InternalId
    ): Flow<ReportReader.Result> {
      val whereClause =
        """
        WHERE ReportRequisitions.DataProviderId = @data_provider_id
          AND ReportRequisitions.CampaignId = @campaign_id
          AND ReportRequisitions.RequisitionId = @requisition_id
        """.trimIndent()
      return ReportRequisitionReader()
        .withBuilder {
          appendClause(whereClause)
          bind("data_provider_id").to(dataProviderId.value)
          bind("campaign_id").to(campaignId.value)
          bind("requisition_id").to(requisitionId.value)
        }
        .execute(readContext)
    }
  }
}
