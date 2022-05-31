package org.wfanet.measurement.reporting.deploy.postgres.readers

import java.sql.Connection
import java.sql.ResultSet
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.internal.reporting.ReportingSet
import org.wfanet.measurement.internal.reporting.ReportingSetKt

class ReportingSetEventGroupReader {
  data class Result(val eventGroupKey: ReportingSet.EventGroupKey)

  private val baseSql: String =
    """
    SELECT
      MeasurementConsumerReferenceId,
      DataProviderReferenceId,
      EventGroupReferenceId
    FROM
      ReportingSetEventGroups
    """

  fun translate(rs: ResultSet): Result = Result(buildEventGroupKey(rs))

  fun listEventGroupKeys(
    connection: Connection,
    measurementConsumerReferenceId: String,
    reportingSetId: InternalId
  ): List<Result> {
    val queryBuilder = StringBuilder(baseSql)
    queryBuilder.append(
      """
        WHERE MeasurementConsumerReferenceId = CAST(? AS text)
          AND ReportingSetId = ?
        """
    )

    val statement = connection.prepareStatement(queryBuilder.toString())
    statement.setString(1, measurementConsumerReferenceId)
    statement.setLong(2, reportingSetId.value)

    val rs = statement.executeQuery()
    statement.use {
      rs.use {
        val resultsList = mutableListOf<Result>()
        while (rs.next()) {
          resultsList.add(translate(rs))
        }
        return resultsList
      }
    }
  }

  private fun buildEventGroupKey(rs: ResultSet): ReportingSet.EventGroupKey {
    return ReportingSetKt.eventGroupKey {
      measurementConsumerReferenceId = rs.getString("MeasurementConsumerReferenceId")
      dataProviderReferenceId = rs.getString("DataProviderReferenceId")
      eventGroupReferenceId = rs.getString("EventGroupReferenceId")
    }
  }
}
