package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.ReadContext
import com.google.cloud.spanner.Statement
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.db.gcp.appendClause
import org.wfanet.measurement.db.gcp.executeSqlQuery
import org.wfanet.measurement.db.kingdom.StreamRequisitionsFilter
import org.wfanet.measurement.internal.kingdom.Requisition

class StreamRequisitionsQuery {
  fun execute(
    readContext: ReadContext,
    filter: StreamRequisitionsFilter,
    limit: Long
  ): Flow<Requisition> {
    val queryBuilder: Statement.Builder =
      REQUISITION_READ_QUERY
        .toBuilder()

    if (!filter.empty) {
      queryBuilder.append("\nWHERE ")
      filter.toSql(queryBuilder, StreamRequisitionsFilterSqlConverter)
    }

    queryBuilder
      .appendClause("ORDER BY CreateTime ASC")
      .appendClause("LIMIT @limit")
      .bind("limit").to(limit)

    return readContext.executeSqlQuery(queryBuilder.build()).map { it.toRequisition() }
  }
}
