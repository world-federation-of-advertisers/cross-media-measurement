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

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.launch
import org.wfanet.measurement.common.db.r2dbc.StatementBuilder
import org.wfanet.measurement.common.db.r2dbc.StatementBuilder.Companion.statementBuilder
import org.wfanet.measurement.internal.reporting.ReportingSet
import org.wfanet.measurement.internal.reporting.copy

class CreateReportingSet(private val reportingSet: ReportingSet) : PostgresWriter<ReportingSet>() {
  override suspend fun TransactionScope.runTransaction(): ReportingSet {
    val internalReportingSetId = idGenerator.generateInternalId().value
    val externalReportingSetId = idGenerator.generateExternalId().value

    val builder =
      statementBuilder(
        """
      INSERT INTO ReportingSets (MeasurementConsumerReferenceId, ReportingSetId, ExternalReportingSetId, Filter, DisplayName)
        VALUES ($1, $2, $3, $4, $5)
      """
      ) {
        bind("$1", reportingSet.measurementConsumerReferenceId)
        bind("$2", internalReportingSetId)
        bind("$3", externalReportingSetId)
        bind("$4", reportingSet.filter)
        bind("$5", reportingSet.displayName)
      }

    transactionContext.run {
      executeStatement(builder).numRowsUpdated
      CoroutineScope(Dispatchers.IO)
        .launch {
          reportingSet.eventGroupKeysList
            .map {
              async {
                executeStatement(createReportingSetEventGroupStatement(it, internalReportingSetId))
                  .numRowsUpdated
              }
            }
            .awaitAll()
        }
        .join()
    }

    return reportingSet.copy { this.externalReportingSetId = externalReportingSetId }
  }

  private fun createReportingSetEventGroupStatement(
    eventGroupKey: ReportingSet.EventGroupKey,
    reportingSetId: Long
  ): StatementBuilder {
    return statementBuilder(
      """
      INSERT INTO ReportingSetEventGroups (MeasurementConsumerReferenceId, DataProviderReferenceId, EventGroupReferenceId, ReportingSetId)
        VALUES ($1, $2, $3, $4)
      """
    ) {
      bind("$1", eventGroupKey.measurementConsumerReferenceId)
      bind("$2", eventGroupKey.dataProviderReferenceId)
      bind("$3", eventGroupKey.eventGroupReferenceId)
      bind("$4", reportingSetId)
    }
  }
}
