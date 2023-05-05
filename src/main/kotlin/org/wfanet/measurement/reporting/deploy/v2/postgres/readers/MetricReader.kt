package org.wfanet.measurement.reporting.deploy.v2.postgres.readers

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import org.wfanet.measurement.common.db.r2dbc.ReadContext
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.internal.reporting.v2.Metric

class MetricReader(private val readContext: ReadContext) {
  data class Result(
    val measurementConsumerId: InternalId,
    val metricId: InternalId,
    val createMetricRequestId: String,
    val metric: Metric
  )

  suspend fun readMetricsByRequestId(
    measurementConsumerId: InternalId,
    createMetricRequestIds: Collection<String>
  ): Flow<Result> {
    if (createMetricRequestIds.isEmpty()) {
      return emptyFlow()
    }

    // TODO(tristanvuong2021): implement read metric
    return emptyFlow()
  }
}
