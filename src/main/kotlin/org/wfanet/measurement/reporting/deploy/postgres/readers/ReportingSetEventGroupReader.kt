package org.wfanet.measurement.reporting.deploy.postgres.readers

import io.r2dbc.spi.Connection
import io.r2dbc.spi.Row
import kotlinx.coroutines.flow.Flow
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
    connection: Connection,
    measurementConsumerReferenceId: String,
    reportingSetId: InternalId
  ): Flow<Result> {
    builder.append(
      """
        WHERE MeasurementConsumerReferenceId = CAST($1 AS text)
          AND ReportingSetId = $2
        """
    )

    val statement =
      connection
        .createStatement(builder.toString())
        .bind("$1", measurementConsumerReferenceId)
        .bind("$2", reportingSetId.value)

    return execute(statement)
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
