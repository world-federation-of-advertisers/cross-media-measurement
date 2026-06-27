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

import io.r2dbc.postgresql.api.PostgresqlException
import io.r2dbc.spi.R2dbcDataIntegrityViolationException
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.internal.reporting.v2.GetOrCreateCampaignGroupReportingSetRequest
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.batchGetReportingSetsRequest
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.MeasurementConsumerReader
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.ReportingSetReader
import org.wfanet.measurement.reporting.service.internal.MeasurementConsumerNotFoundException
import org.wfanet.measurement.reporting.service.internal.ReportingSetAlreadyExistsException

/**
 * Reuses or inserts the synthesized campaign-group [ReportingSet] for a custom-group BasicReport.
 *
 * The read-by-content and the insert happen in a single transaction (the [PostgresWriter] commit is
 * wrapped in serializable retries by [PostgresWriter.execute]), so concurrent and retried callers
 * converge on a single campaign-group ReportingSet:
 * * the lookup observes whether an identical campaign group already exists for the
 *   `MeasurementConsumer`,
 * * if it does, that ReportingSet is reused,
 * * otherwise a new one is inserted; a concurrent racer that inserted first causes a serialization
 *   conflict, and the retry observes the winner and reuses it.
 *
 * [GetOrCreateCampaignGroupReportingSetRequest.reportingSet] must be a primitive, self-referencing
 * campaign group with at least one EventGroup; this is enforced by the service before the writer
 * runs.
 *
 * Throws the following on [execute]:
 * * [MeasurementConsumerNotFoundException] MeasurementConsumer not found
 * * [ReportingSetAlreadyExistsException] the generated external ID collided with an existing one
 */
class GetOrCreateCampaignGroupReportingSet(
  private val request: GetOrCreateCampaignGroupReportingSetRequest
) : PostgresWriter<ReportingSet>() {
  override suspend fun TransactionScope.runTransaction(): ReportingSet {
    val reportingSet = request.reportingSet
    val cmmsMeasurementConsumerId = reportingSet.cmmsMeasurementConsumerId
    val measurementConsumerId =
      (MeasurementConsumerReader(transactionContext).getByCmmsId(cmmsMeasurementConsumerId)
          ?: throw MeasurementConsumerNotFoundException(cmmsMeasurementConsumerId))
        .measurementConsumerId

    val eventGroupKeys = reportingSet.primitive.eventGroupKeysList

    val existing: ReportingSetReader.ReportingSetIds? =
      ReportingSetReader(transactionContext)
        .readCampaignGroupByEventGroups(measurementConsumerId, eventGroupKeys)

    val externalReportingSetId: String =
      if (existing != null) {
        existing.externalReportingSetId
      } else {
        insertCampaignGroup(measurementConsumerId)
        request.externalReportingSetId
      }

    return ReportingSetReader(transactionContext)
      .batchGetReportingSets(
        batchGetReportingSetsRequest {
          this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
          externalReportingSetIds += externalReportingSetId
        }
      )
      .map { it.reportingSet }
      .first()
  }

  private suspend fun TransactionScope.insertCampaignGroup(measurementConsumerId: InternalId) {
    val reportingSet = request.reportingSet
    val reportingSetId = idGenerator.generateInternalId()
    val externalReportingSetId = request.externalReportingSetId

    val statement =
      boundStatement(
        """
        INSERT INTO ReportingSets
          (
            MeasurementConsumerId,
            ReportingSetId,
            ExternalReportingSetId,
            DisplayName,
            Filter,
            ReportingSetDetails,
            ReportingSetDetailsJson,
            CampaignGroupId
          )
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """
      ) {
        bind("$1", measurementConsumerId)
        bind("$2", reportingSetId)
        bind("$3", externalReportingSetId)
        bind("$4", reportingSet.displayName)
        bind("$5", reportingSet.filter)
        bind("$6", reportingSet.details)
        bind("$7", reportingSet.details.toJson())
        // Self-referencing: the synthesized campaign group is its own campaign group.
        bind("$8", reportingSetId)
      }

    try {
      transactionContext.executeStatement(statement)
    } catch (e: R2dbcDataIntegrityViolationException) {
      if (e is PostgresqlException && e.errorDetails.code == INTEGRITY_CONSTRAINT_VIOLATION) {
        throw ReportingSetAlreadyExistsException()
      } else {
        throw e
      }
    }

    insertReportingSetEventGroups(
      measurementConsumerId,
      reportingSetId,
      reportingSet.primitive.eventGroupKeysList,
    )
  }
}
