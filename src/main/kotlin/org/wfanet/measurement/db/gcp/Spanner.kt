package org.wfanet.measurement.db.gcp

import com.google.cloud.spanner.DatabaseClient
import com.google.cloud.spanner.ReadContext
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.TransactionContext
import kotlinx.coroutines.channels.ReceiveChannel

/**
 * Wraps the Java-centric Google Cloud Spanner library to make it more convenient for Kotlin.
 */
interface Spanner {

  /**
   * Executes an SQL read query.
   *
   * @param[sql] the query to execute
   * @param[reader] the context in which to execute it (e.g. transaction or standalone)
   * @return a [ReceiveChannel] with the results of the query
   */
  suspend fun executeSqlQuery(sql: Statement, reader: ReadContext): ReceiveChannel<Struct>

  /**
   * Runs a Spanner transaction.
   *
   * @param[dbClient] the connection to the database
   * @param[block] the body of the transaction
   */
  suspend fun transaction(dbClient: DatabaseClient, block: (TransactionContext) -> Unit)
}
