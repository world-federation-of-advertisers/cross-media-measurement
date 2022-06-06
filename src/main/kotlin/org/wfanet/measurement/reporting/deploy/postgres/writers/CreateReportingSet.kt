// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.deploy.postgres.writers

import io.r2dbc.spi.Connection
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactive.awaitFirst
import org.wfanet.measurement.internal.reporting.ReportingSet
import org.wfanet.measurement.internal.reporting.copy

class CreateReportingSet(private val reportingSet: ReportingSet) : PostgresWriter<ReportingSet>() {
  override suspend fun TransactionScope.runTransaction(): ReportingSet {
    val internalReportingSetId = idGenerator.generateInternalId().value
    val externalReportingSetId = idGenerator.generateExternalId().value

    println(
      "rows updated:" +
        connection
          .createStatement(
            """
      INSERT INTO ReportingSets (MeasurementConsumerReferenceId, ReportingSetId, ExternalReportingSetId, Filter, DisplayName)
        VALUES ($1, $2, $3, $4, $5)
      """
          )
          .bind("$1", reportingSet.measurementConsumerReferenceId)
          .bind("$2", internalReportingSetId)
          .bind("$3", externalReportingSetId)
          .bind("$4", reportingSet.filter)
          .bind("$5", reportingSet.displayName)
          .execute()
          .awaitFirst()
          .rowsUpdated.awaitFirst()
    )

    CoroutineScope(Dispatchers.IO)
      .launch {
        reportingSet.eventGroupKeysList
          .map { async { insertReportingSetEventGroup(connection, it, internalReportingSetId) } }
          .awaitAll()
      }
      .join()

    return reportingSet.copy { this.externalReportingSetId = externalReportingSetId }
  }

  private suspend fun insertReportingSetEventGroup(
    connection: Connection,
    eventGroupKey: ReportingSet.EventGroupKey,
    reportingSetId: Long
  ) {
    connection
      .createStatement(
        """
      INSERT INTO ReportingSetEventGroups (MeasurementConsumerReferenceId, DataProviderReferenceId, EventGroupReferenceId, ReportingSetId)
        VALUES ($1, $2, $3, $4)
      """
      )
      .bind("$1", eventGroupKey.eventGroupReferenceId)
      .bind("$2", eventGroupKey.dataProviderReferenceId)
      .bind("$3", eventGroupKey.eventGroupReferenceId)
      .bind("$4", reportingSetId)
      .execute()
      .awaitFirst()
      .rowsUpdated.awaitFirst()
  }
}
