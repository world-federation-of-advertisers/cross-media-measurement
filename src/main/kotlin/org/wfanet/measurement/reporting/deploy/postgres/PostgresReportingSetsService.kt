package org.wfanet.measurement.reporting.deploy.postgres

import java.sql.Connection
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.internal.reporting.ReportingSet
import org.wfanet.measurement.internal.reporting.ReportingSetsGrpcKt
import org.wfanet.measurement.internal.reporting.StreamReportingSetsRequest
import org.wfanet.measurement.reporting.deploy.postgres.readers.ReportingSetReader
import org.wfanet.measurement.reporting.deploy.postgres.writers.CreateReportingSet

class PostgresReportingSetsService(
  private val idGenerator: IdGenerator,
  private val getConnection: () -> Connection
) : ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase() {
  override suspend fun createReportingSet(request: ReportingSet): ReportingSet {
    val connection = getConnection()
    connection.isReadOnly = false
    connection.autoCommit = false
    connection.use {
      try {
        val reportingSet = CreateReportingSet(request).execute(it, idGenerator)
        connection.commit()
        return reportingSet
      } catch (e: Exception) {
        connection.rollback()
        throw e
      }
    }
  }

  override fun streamReportingSets(request: StreamReportingSetsRequest): Flow<ReportingSet> {
    val connection = getConnection()
    connection.isReadOnly = true
    connection.use {
      return ReportingSetReader()
        .listReportingSets(connection, request.filter, request.limit)
        .asFlow()
        .map { result -> result.reportingSet }
    }
  }
}
