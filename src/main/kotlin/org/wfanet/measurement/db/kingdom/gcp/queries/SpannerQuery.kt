package org.wfanet.measurement.db.kingdom.gcp.queries

import com.google.cloud.spanner.ReadContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.db.gcp.spannerDispatcher
import org.wfanet.measurement.db.kingdom.gcp.readers.BaseSpannerReader

/**
 * Abstraction around a common pattern for Spanner read-only queries.
 */
abstract class SpannerQuery<S : Any, T> {
  /**
   * The [BaseSpannerReader] to execute.
   */
  protected abstract val reader: BaseSpannerReader<S>

  /**
   * Post-processes the results.
   */
  abstract fun Flow<S>.transform(): Flow<T>

  /**
   * Executes the query.
   */
  fun execute(readContext: ReadContext): Flow<T> {
    return reader.execute(readContext).transform()
  }

  /**
   * Executes the query to take the unique result, running on the proper Dispatcher.
   */
  fun executeSingle(readContext: ReadContext): T = runBlocking(spannerDispatcher()) {
    execute(readContext).single()
  }
}
