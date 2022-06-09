package org.wfanet.measurement.reporting.deploy.postgres.readers

import io.r2dbc.spi.Row
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import org.wfanet.measurement.common.db.r2dbc.ReadContext
import org.wfanet.measurement.common.db.r2dbc.StatementBuilder.Companion.statementBuilder
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.internal.reporting.ReportingSet
import org.wfanet.measurement.internal.reporting.ReportingSetKt

class ReportingSetEventGroupReader : PostgresReader<ReportingSetEventGroupReader.Result>() {
  data class Result(val eventGroupKey: ReportingSet.EventGroupKey)

  override val baseSql: String =
    """
    SELECT
      MeasurementConsumerReferenceId,
      DataProviderReferenceId,
      EventGroupReferenceId
    FROM
      ReportingSetEventGroups
    """

  override fun translate(row: Row): Result = Result(buildEventGroupKey(row))

  fun listEventGroupKeys(
    readContext: ReadContext,
    measurementConsumerReferenceId: String,
    reportingSetId: InternalId
  ): Flow<Result> {
    return flow {
      val builder =
        statementBuilder(
          baseSql +
            """
        WHERE MeasurementConsumerReferenceId = CAST($1 AS text)
          AND ReportingSetId = $2
        """
        ) {
          bind("$1", measurementConsumerReferenceId)
          bind("$2", reportingSetId.value)
        }

      execute(readContext, builder).collect { emit(it) }
    }
  }

  private fun buildEventGroupKey(row: Row): ReportingSet.EventGroupKey {
    return ReportingSetKt.eventGroupKey {
      measurementConsumerReferenceId =
        row.get("MeasurementConsumerReferenceId", String::class.java) as String
      dataProviderReferenceId = row.get("DataProviderReferenceId", String::class.java) as String
      eventGroupReferenceId = row.get("EventGroupReferenceId", String::class.java) as String
    }
  }
}
