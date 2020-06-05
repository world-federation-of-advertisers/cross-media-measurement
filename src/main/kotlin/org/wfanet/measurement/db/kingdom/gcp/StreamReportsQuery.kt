package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.ReadContext
import kotlinx.coroutines.flow.Flow
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

    reader.builder
      .appendClause("ORDER BY CreateTime ASC")
      .appendClause("LIMIT @limit")
      .bind("limit").to(limit)
      .build()

    return reader.execute(readContext)
  }
}
