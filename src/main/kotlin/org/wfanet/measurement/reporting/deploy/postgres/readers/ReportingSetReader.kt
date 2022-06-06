package org.wfanet.measurement.reporting.deploy.postgres.readers

import io.r2dbc.spi.Connection
import io.r2dbc.spi.Row
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.internal.reporting.ReportingSet
import org.wfanet.measurement.internal.reporting.StreamReportingSetsRequest
import org.wfanet.measurement.internal.reporting.copy
import org.wfanet.measurement.internal.reporting.reportingSet

class ReportingSetReader : PostgresReader<ReportingSetReader.Result>() {
  data class Result(
    val measurementConsumerReferenceId: String,
    val reportingSetId: InternalId,
    var reportingSet: ReportingSet
  )

  override val baseSql: String =
    """
    SELECT
      MeasurementConsumerReferenceId,
      ReportingSetId,
      ExternalReportingSetId,
      Filter,
      DisplayName
    FROM
      ReportingSets
    """

  override fun translate(connection: Connection, row: Row): Result =
    Result(
      row.get("MeasurementConsumerReferenceId", String::class.java) as String,
      InternalId(row.get("ReportingSetId", java.lang.Long::class.java) as Long),
      buildReportingSet(row)
    )

  fun listReportingSets(
    filter: StreamReportingSetsRequest.Filter,
    limit: Int = 0,
    getConnection: suspend () -> Connection,
  ): Flow<Result> {
    builder.appendLine(
      """
      WHERE MeasurementConsumerReferenceId = CAST($1 AS text)
        AND ExternalReportingSetId > $2
      """
    )
    builder.appendLine("ORDER BY ExternalReportingSetId ASC")
    builder.appendLine("LIMIT $3")

    return flow {
      val connection = getConnection()
      try {
        val statement =
          connection
            .createStatement(builder.toString())
            .bind("$1", filter.measurementConsumerReferenceId)
            .bind("$2", filter.externalReportingSetIdAfter)

        if (limit > 0) {
          statement.bind("$3", limit)
        } else {
          statement.bind("$3", 50)
        }

        execute(connection, statement).asFlow().collect { reportingSetResult ->
          reportingSetResult.reportingSet =
            reportingSetResult.reportingSet.copy {
              ReportingSetEventGroupReader()
                .listEventGroupKeys(
                  connection,
                  reportingSetResult.measurementConsumerReferenceId,
                  reportingSetResult.reportingSetId
                )
                .collect { eventGroupResult -> eventGroupKeys += eventGroupResult.eventGroupKey }
            }
          emit(reportingSetResult)
        }
      } finally {
        connection.close().awaitFirstOrNull()
      }
    }
  }

  private fun buildReportingSet(row: Row): ReportingSet {
    return reportingSet {
      measurementConsumerReferenceId =
        row.get("MeasurementConsumerReferenceId", String::class.java) as String
      externalReportingSetId = row.get("ExternalReportingSetId", java.lang.Long::class.java) as Long
      filter = row.get("Filter", String::class.java) as String
      displayName = row.get("DisplayName", String::class.java) as String
    }
  }
}
