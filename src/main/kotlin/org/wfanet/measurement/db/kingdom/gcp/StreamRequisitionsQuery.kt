package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.ReadContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.db.gcp.appendClause
import org.wfanet.measurement.db.kingdom.StreamRequisitionsFilter
import org.wfanet.measurement.internal.kingdom.Requisition

class StreamRequisitionsQuery {
  /**
   * Streams [Requisition]s matching [filter] from Spanner.
   *
   * @param[readContext] the context in which to perform Spanner reads
   * @param[filter] a filter to control which [Requisition]s to return
   * @param[limit] how many [Requisition]s to return -- if zero, there is no limit
   * @return a [Flow] of [Requisition]s matching the filter ordered by ascending CreateTime
   */
  fun execute(
    readContext: ReadContext,
    filter: StreamRequisitionsFilter,
    limit: Long
  ): Flow<Requisition> {
    val reader = RequisitionReader()

    if (!filter.empty) {
      reader.builder.append("\nWHERE ")
      filter.toSql(reader.builder, StreamRequisitionsFilterSqlConverter)
    }

    reader.builder
      .appendClause("ORDER BY CreateTime ASC")
      .appendClause("LIMIT @limit")
      .bind("limit").to(limit)

    return reader.execute(readContext).map { it.requisition }
  }
}
