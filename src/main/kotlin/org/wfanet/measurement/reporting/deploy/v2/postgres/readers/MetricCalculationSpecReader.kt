package org.wfanet.measurement.reporting.deploy.v2.postgres.readers

import kotlinx.coroutines.flow.singleOrNull
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.db.r2dbc.ReadContext
import org.wfanet.measurement.common.db.r2dbc.ResultRow
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.internal.reporting.v2.ListMetricCalculationSpecsRequest
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.metricCalculationSpec

private const val LIST_DEFAULT_LIMIT = 50

class MetricCalculationSpecReader(private val readContext: ReadContext) {
  data class Result(
    val measurementConsumerId: InternalId,
    val metricCalculationSpecId: InternalId,
    val metricCalculationSpec: MetricCalculationSpec
  )

  private val baseSql: String =
    """
    SELECT
      CmmsMeasurementConsumerId,
      MeasurementConsumerId,
      MetricCalculationSpecId,
      ExternalMetricCalculationSpecId,
      MetricCalculationSpecDetails,
    FROM MetricCalculationSpecs
      JOIN MeasurementConsumers USING(MeasurementConsumerId)
    """
      .trimIndent()

  fun translate(row: ResultRow): Result =
    Result(row["MeasurementConsumerId"], row["MetricCalculationSpecId"], buildMetricCalculationSpec(row))

  suspend fun readMetricCalculationSpecByExternalId(
    cmmsMeasurementConsumerId: String,
    externalMetricCalculationSpecId: String,
  ): Result? {
    val sql =
      StringBuilder(
        baseSql +
          """
           WHERE CmmsMeasurementConsumerId = $1
          AND ExternalMetricCalculationSpecId = $2
          """
            .trimIndent()
      )

    val statement =
      boundStatement(sql.toString()) {
        bind("$1", cmmsMeasurementConsumerId)
        bind("$2", externalMetricCalculationSpecId)
      }

    return readContext.executeQuery(statement).consume(::translate).singleOrNull()
  }

  suspend fun readMetricCalculationSpecs(
    request: ListMetricCalculationSpecsRequest
  ): List<Result> {
    val sql =
      StringBuilder(
        baseSql +
          """
           WHERE CmmsMeasurementConsumerId = $1
          AND ExternalMetricCalculationSpecId > $2
          ORDER BY ExternalMetricCalculationSpecId ASC
          LIMIT $3
          """
            .trimIndent()
      )

    val statement =
      boundStatement(sql.toString()) {
        bind("$1", request.filter.cmmsMeasurementConsumerId)
        bind("$2", request.filter.externalMetricCalculationSpecIdAfter)

        if (request.limit > 0) {
          bind("$3", request.limit)
        } else {
          bind("$3", LIST_DEFAULT_LIMIT)
        }
      }

    return readContext.executeQuery(statement).consume(::translate).toList()
  }

  private fun buildMetricCalculationSpec(row: ResultRow): MetricCalculationSpec {
    return metricCalculationSpec {
      cmmsMeasurementConsumerId = row["CmmsMeasurementConsumerId"]
      externalMetricCalculationSpecId = row["ExternalMetricCalculationSpecId"]
      details = row.getProtoMessage("MetricCalculationSpecDetails", MetricCalculationSpec.Details.parser())
    }
  }
}
