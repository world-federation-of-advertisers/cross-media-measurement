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

import java.sql.Connection
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.internal.reporting.ReportingSet
import org.wfanet.measurement.internal.reporting.copy

class CreateReportingSet(private val reportingSet: ReportingSet) {
  fun execute(connection: Connection, idGenerator: IdGenerator): ReportingSet {
    val internalReportingSetId = idGenerator.generateInternalId().value
    val externalReportingSetId = idGenerator.generateExternalId().value

    connection
      .prepareStatement(
        """
      INSERT INTO ReportingSets (MeasurementConsumerReferenceId, ReportingSetId, ExternalReportingSetId, Filter, DisplayName)
        VALUES (?, ?, ?, ?, ?)
      """
      )
      .use { statement ->
        statement.setString(1, reportingSet.measurementConsumerReferenceId)
        statement.setLong(2, internalReportingSetId)
        statement.setLong(3, externalReportingSetId)
        statement.setString(4, reportingSet.filter)
        statement.setString(5, reportingSet.displayName)
        statement.executeUpdate()
      }

    reportingSet.eventGroupKeysList.forEach {
      insertReportingSetEventGroup(connection, it, internalReportingSetId)
    }

    return reportingSet.copy { this.externalReportingSetId = externalReportingSetId }
  }

  private fun insertReportingSetEventGroup(
    connection: Connection,
    eventGroupKey: ReportingSet.EventGroupKey,
    reportingSetId: Long
  ) {
    connection
      .prepareStatement(
        """
      INSERT INTO ReportingSetEventGroups (MeasurementConsumerReferenceId, DataProviderReferenceId, EventGroupReferenceId, ReportingSetId)
        VALUES (?, ?, ?, ?)
      """
      )
      .use { statement ->
        statement.setString(1, eventGroupKey.measurementConsumerReferenceId)
        statement.setString(2, eventGroupKey.dataProviderReferenceId)
        statement.setString(3, eventGroupKey.eventGroupReferenceId)
        statement.setLong(4, reportingSetId)
        statement.executeUpdate()
      }
  }
}
