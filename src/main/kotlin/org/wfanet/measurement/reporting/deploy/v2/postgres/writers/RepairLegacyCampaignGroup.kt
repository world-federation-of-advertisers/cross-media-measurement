/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.reporting.deploy.v2.postgres.writers

import kotlinx.coroutines.flow.single
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.batchGetReportingSetsRequest
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.ReportingSetReader
import org.wfanet.measurement.reporting.service.internal.CampaignGroupInvalidException
import org.wfanet.measurement.reporting.service.internal.ReportingSetNotFoundException

/**
 * Repairs a legacy primitive ReportingSet that is referenced as a Campaign Group but has no
 * Campaign Group association.
 *
 * A repair sets `CampaignGroupId` to the ReportingSet's own internal ID. ReportingSets that are
 * composites, have no EventGroups, or already belong to a different Campaign Group are rejected.
 * Existing self-referencing Campaign Groups are returned unchanged.
 *
 * @param dryRun when true, validates the repair without updating the database
 * @return the ReportingSet as it existed before any repair
 * @throws CampaignGroupInvalidException if the ReportingSet cannot be safely repaired
 * @throws ReportingSetNotFoundException if the ReportingSet does not exist
 */
class RepairLegacyCampaignGroup(
  private val cmmsMeasurementConsumerId: String,
  private val externalReportingSetId: String,
  private val dryRun: Boolean,
) : PostgresWriter<ReportingSet>() {
  override suspend fun TransactionScope.runTransaction(): ReportingSet {
    val result: ReportingSetReader.Result =
      ReportingSetReader(transactionContext)
        .batchGetReportingSets(
          batchGetReportingSetsRequest {
            this.cmmsMeasurementConsumerId =
              this@RepairLegacyCampaignGroup.cmmsMeasurementConsumerId
            externalReportingSetIds += externalReportingSetId
          }
        )
        .single()
    val reportingSet: ReportingSet = result.reportingSet

    if (reportingSet.externalCampaignGroupId == externalReportingSetId) {
      return reportingSet
    }
    if (
      reportingSet.externalCampaignGroupId.isNotEmpty() ||
        !reportingSet.hasPrimitive() ||
        reportingSet.primitive.eventGroupKeysCount == 0
    ) {
      throw CampaignGroupInvalidException(cmmsMeasurementConsumerId, externalReportingSetId)
    }

    if (!dryRun) {
      val updateResult =
        transactionContext.executeStatement(
          boundStatement(
            """
            UPDATE ReportingSets
            SET CampaignGroupId = ReportingSetId
            WHERE MeasurementConsumerId = $1
              AND ReportingSetId = $2
              AND CampaignGroupId IS NULL
            """
              .trimIndent()
          ) {
            bind("$1", result.measurementConsumerId)
            bind("$2", result.reportingSetId)
          }
        )
      check(updateResult.numRowsUpdated == 1L) {
        "Legacy Campaign Group repair did not update exactly one ReportingSet"
      }
    }

    return reportingSet
  }
}
