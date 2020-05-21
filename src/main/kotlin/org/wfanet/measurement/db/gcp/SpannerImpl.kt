package org.wfanet.measurement.db.gcp

import com.google.cloud.spanner.DatabaseClient
import com.google.cloud.spanner.ReadContext
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.TransactionContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.withContext

/**
 * Coroutine-friendly implementation of [Spanner].
 *
 * Since the database operations are blocking and not coroutine-friendly, they should be run in an
 * isolated thread pool, given by [scope].
 *
 * @param[scope] the scope to run database in.
 */
@OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
class SpannerImpl(private val scope: CoroutineScope) :
  Spanner {
  override suspend fun executeSqlQuery(
    sql: Statement,
    reader: ReadContext
  ): ReceiveChannel<Struct> = scope.produce {
    val resultSet = reader.executeQuery(sql)
    while (resultSet.next()) {
      send(resultSet.currentRowAsStruct)
    }
  }

  override suspend fun transaction(
    dbClient: DatabaseClient,
    block: (TransactionContext) -> Unit
  ) {
    withContext(scope.coroutineContext) {
      dbClient.readWriteTransaction().run(block)
    }
  }
}
