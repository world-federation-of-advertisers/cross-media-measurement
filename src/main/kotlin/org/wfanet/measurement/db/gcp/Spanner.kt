package org.wfanet.measurement.db.gcp

import com.google.cloud.spanner.DatabaseClient
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.TransactionContext
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers

/**
 * Dispatcher for blocking database operations.
 */
fun spannerDispatcher(): CoroutineDispatcher = Dispatchers.IO

/**
 * Executes a RMW transaction.
 *
 * This wraps the Java API to be more convenient for Kotlin because the Java API is nullable but the
 * block given isn't, so coercion from nullable to not is needed.
 *
 * @param[block] the body of the transaction
 * @return the result of [block]
 */
fun <T> DatabaseClient.runReadWriteTransaction(
  block: (TransactionContext) -> T
): T = readWriteTransaction().run(block)!!

/**
 * Convenience function for appending without worrying about whether the last [append] had
 * sufficient whitespace -- this adds a newline before and a space after.
 */
fun Statement.Builder.appendClause(sql: String): Statement.Builder = append("\n$sql ")
