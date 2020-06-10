package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.ReadContext
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.db.gcp.asFlow

/**
 * Abstraction for reading rows from Spanner and translating into more expressive objects.
 */
abstract class SpannerReader<T> {
  protected abstract val baseSql: String

  protected abstract suspend fun translate(struct: Struct): T

  val builder: Statement.Builder by lazy {
    Statement.newBuilder(baseSql)
  }

  fun withBuilder(block: Statement.Builder.() -> Unit): SpannerReader<T> {
    builder.block()
    return this
  }

  fun execute(readContext: ReadContext): Flow<T> =
    readContext.executeQuery(builder.build()).asFlow().map(::translate)
}
