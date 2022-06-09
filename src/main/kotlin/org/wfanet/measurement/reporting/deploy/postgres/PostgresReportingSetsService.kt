package org.wfanet.measurement.reporting.deploy.postgres

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.db.r2dbc.DatabaseClient
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.internal.reporting.ReportingSet
import org.wfanet.measurement.internal.reporting.ReportingSetsGrpcKt
import org.wfanet.measurement.internal.reporting.StreamReportingSetsRequest
import org.wfanet.measurement.reporting.deploy.postgres.readers.ReportingSetReader
import org.wfanet.measurement.reporting.deploy.postgres.writers.CreateReportingSet

class PostgresReportingSetsService(
  private val idGenerator: IdGenerator,
  private val client: DatabaseClient,
) : ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase() {
  override suspend fun createReportingSet(request: ReportingSet): ReportingSet {
    return CreateReportingSet(request).execute(client, idGenerator)
  }

  override fun streamReportingSets(request: StreamReportingSetsRequest): Flow<ReportingSet> {
    return ReportingSetReader().listReportingSets(client, request.filter, request.limit).map {
      result ->
      result.reportingSet
    }
  }
}
