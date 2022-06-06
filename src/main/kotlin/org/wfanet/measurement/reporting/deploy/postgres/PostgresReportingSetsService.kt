package org.wfanet.measurement.reporting.deploy.postgres

import io.r2dbc.spi.Connection
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.internal.reporting.ReportingSet
import org.wfanet.measurement.internal.reporting.ReportingSetsGrpcKt
import org.wfanet.measurement.internal.reporting.StreamReportingSetsRequest
import org.wfanet.measurement.reporting.deploy.postgres.readers.ReportingSetReader
import org.wfanet.measurement.reporting.deploy.postgres.writers.CreateReportingSet

class PostgresReportingSetsService(
  private val idGenerator: IdGenerator,
  private val getConnection: suspend () -> Connection
) : ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase() {
  override suspend fun createReportingSet(request: ReportingSet): ReportingSet {
    val connection = getConnection()
    try {
      return CreateReportingSet(request).execute(connection, idGenerator)
    } finally {
      connection.close().awaitFirstOrNull()
    }
  }

  override fun streamReportingSets(request: StreamReportingSetsRequest): Flow<ReportingSet> {
    return ReportingSetReader()
      .listReportingSets(request.filter, request.limit, getConnection)
      .map { result -> result.reportingSet }
  }
}
