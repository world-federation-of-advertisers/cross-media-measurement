package org.wfanet.measurement.db.gcp

import com.google.cloud.spanner.DatabaseClient
import com.google.cloud.spanner.ReadContext
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.TransactionContext
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

/**
 * Dispatcher for blocking database operations.
 */
fun spannerDispatcher(): CoroutineDispatcher = Dispatchers.IO

/**
 * Executes an SQL query, sending the results to a channel.
 *
 * @param[sql] the query to run
 * @return a [ReceiveChannel] with the results of the query
 */
@OptIn(ExperimentalCoroutinesApi::class)
fun ReadContext.executeSqlQuery(sql: Statement): Flow<Struct> = flow {
  val resultSet = executeQuery(sql)
  while (resultSet.next()) {
    emit(resultSet.currentRowAsStruct)
  }
}

fun ReadContext.executeSqlQuery(sql: String): Flow<Struct> =
  executeSqlQuery(Statement.of(sql))

/**
 * Executes a RMW transaction.
 *
 * This wraps the Java API to be more convenient for Kotlin because the Java API is nullable but the
 * block given isn't, so coercion from nullable to not is needed.
 *
 * @param[block] the body of the transaction
 * @return the result of [block]
 */
@OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
fun <T> DatabaseClient.runReadWriteTransaction(
  block: (TransactionContext) -> T
): T = readWriteTransaction().run(block)!!

/**
 * Convenience function for appending without worrying about whether the last [append] had
 * sufficient whitespace -- this adds a newline before and a space after.
 */
fun Statement.Builder.appendClause(sql: String): Statement.Builder = append("\n$sql ")
