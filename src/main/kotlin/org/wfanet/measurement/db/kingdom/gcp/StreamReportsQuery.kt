package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.ReadContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.db.gcp.appendClause
import org.wfanet.measurement.db.kingdom.StreamReportsFilter
import org.wfanet.measurement.internal.kingdom.Report

class StreamReportsQuery {
  fun execute(
    readContext: ReadContext,
    filter: StreamReportsFilter,
    limit: Long
  ): Flow<Report> {
    val reader = ReportReader()

    if (!filter.empty) {
      reader.builder.append("\nWHERE ")
      filter.toSql(reader.builder, StreamReportsFilterSqlConverter)
    }

    reader.builder.appendClause("ORDER BY UpdateTime ASC")

    if (limit > 0) {
      reader.builder
        .appendClause("LIMIT @limit")
        .bind("limit").to(limit)
    }

    return reader.execute(readContext).map { it.report }
  }
}
