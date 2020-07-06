package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.ReadContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.db.gcp.appendClause
import org.wfanet.measurement.db.kingdom.StreamReportsFilter
import org.wfanet.measurement.internal.kingdom.Report

class StreamReportsQuery {
  /**
   * Streams [Report]s matching [filter] from Spanner.
   *
   * @param[readContext] the context in which to perform Spanner reads
   * @param[filter] a filter to control which [Report]s to return
   * @param[limit] how many [Report]s to return -- if zero, there is no limit
   * @return a [Flow] of [Report]s matching the filter ordered by ascending UpdateTime
   */
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
