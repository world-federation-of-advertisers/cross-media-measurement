package org.wfanet.measurement.reporting.deploy.postgres.readers

import java.sql.Connection
import java.sql.ResultSet
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.internal.reporting.ReportingSet
import org.wfanet.measurement.internal.reporting.StreamReportingSetsRequest
import org.wfanet.measurement.internal.reporting.reportingSet

class ReportingSetReader {
  data class Result(
    val measurementConsumerReferenceId: String,
    val reportingSetId: InternalId,
    val reportingSet: ReportingSet
  )

  private val baseSql: String =
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

  private fun translate(connection: Connection, rs: ResultSet): Result =
    Result(
      rs.getString("MeasurementConsumerReferenceId"),
      InternalId(rs.getLong("ReportingSetId")),
      buildReportingSet(connection, rs)
    )

  fun listReportingSets(
    connection: Connection,
    filter: StreamReportingSetsRequest.Filter,
    limit: Int = 0,
  ): List<Result> {
    val queryBuilder = StringBuilder(baseSql)
    queryBuilder.appendLine(
      """
      WHERE MeasurementConsumerReferenceId = CAST(? AS text)
        AND ExternalReportingSetId > ?
      """
    )
    queryBuilder.appendLine("ORDER BY ExternalReportingSetId ASC")
    queryBuilder.appendLine("LIMIT ?")

    val statement = connection.prepareStatement(queryBuilder.toString())
    statement.setString(1, filter.measurementConsumerReferenceId)
    statement.setLong(2, filter.externalReportingSetIdAfter)
    if (limit > 0) {
      statement.setInt(3, limit)
    } else {
      statement.setInt(3, 50)
    }

    val rs = statement.executeQuery()
    statement.use {
      rs.use {
        val resultsList = mutableListOf<Result>()
        while (rs.next()) {
          resultsList.add(translate(connection, rs))
        }
        return resultsList
      }
    }
  }

  private fun buildReportingSet(connection: Connection, rs: ResultSet): ReportingSet {
    return reportingSet {
      measurementConsumerReferenceId = rs.getString("MeasurementConsumerReferenceId")
      externalReportingSetId = rs.getLong("ExternalReportingSetId")
      filter = rs.getString("Filter")
      displayName = rs.getString("DisplayName")
      ReportingSetEventGroupReader()
        .listEventGroupKeys(
          connection,
          measurementConsumerReferenceId,
          InternalId(rs.getLong("ReportingSetId"))
        )
        .forEach { eventGroupKeys += it.eventGroupKey }
    }
  }
}
