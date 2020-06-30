package org.wfanet.measurement.db.duchy.gcp

import com.google.cloud.spanner.DatabaseClient
import com.google.cloud.spanner.ReadContext
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.db.gcp.asFlow

/**
 * Wrapper around an SQL based query to the Spanner database that abstracts away spanner
 * result sets and spanner structs.
 */
interface SqlBasedQuery<out Result> {
  val sql: Statement

  /** Transmogrify a single resulting row [Struct] in the [Result] type. */
  fun asResult(struct: Struct): Result

  /**
   *  Runs this query using a singleUse query in the database client, returning a [Sequence]
   *  of the [Result]s.
   */
  fun execute(databaseClient: DatabaseClient): Flow<Result> =
    execute(databaseClient.singleUse())

  /** Runs this query using a read context, returning a [Sequence] of the [Result]s. */
  fun execute(readContext: ReadContext): Flow<Result> =
    readContext.executeQuery(sql).asFlow().map { asResult(it) }
}
