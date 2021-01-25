// Copyright 2020 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.gcloud.spanner

import com.google.api.core.ApiFuture
import com.google.api.core.ApiFutures
import com.google.cloud.Timestamp
import com.google.cloud.spanner.AsyncResultSet
import com.google.cloud.spanner.AsyncRunner
import com.google.cloud.spanner.DatabaseClient
import com.google.cloud.spanner.Key
import com.google.cloud.spanner.KeySet
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Options.QueryOption
import com.google.cloud.spanner.Options.ReadOption
import com.google.cloud.spanner.ReadContext
import com.google.cloud.spanner.SpannerException
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.TimestampBound
import com.google.cloud.spanner.TransactionContext
import java.time.Duration
import java.util.concurrent.Executor
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asExecutor
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.sendBlocking
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.singleOrNull
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.wfanet.measurement.gcloud.common.asDeferred

private val executor: Executor = Dispatchers.IO.asExecutor()
private typealias AsyncWork<T> = suspend (TransactionContext) -> ApiFuture<T>
typealias TransactionWork<R> = suspend (txn: AsyncDatabaseClient.TransactionContext) -> R

/**
 * Non-blocking wrapper around [dbClient] using asynchronous Spanner Java API.
 *
 * This class only exposes the methods used by this project and not the complete
 * API.
 */
class AsyncDatabaseClient(private val dbClient: DatabaseClient) {
  /** @see [DatabaseClient.singleUse] */
  fun singleUse(bound: TimestampBound = TimestampBound.strong()): ReadContext {
    return ReadContextImpl(dbClient.singleUse(bound))
  }

  /** @see [DatabaseClient.readWriteTransaction] */
  fun readWriteTransaction(): TransactionRunner {
    return TransactionRunnerImpl(dbClient.runAsync())
  }

  /** @see [DatabaseClient.write] */
  suspend fun write(mutations: Iterable<Mutation>) {
    readWriteTransaction().execute { txn ->
      txn.buffer(mutations)
    }
  }

  /** @see [DatabaseClient.write] */
  suspend fun write(vararg mutations: Mutation) {
    write(mutations.asIterable())
  }

  /**
   * Suspends until the [AsyncDatabaseClient] is ready, throwing a
   * [kotlinx.coroutines.TimeoutCancellationException] on timeout.
   */
  suspend fun waitUntilReady(timeout: Duration) {
    // Issue a no-op query and attempt to get the result in order to verify that
    // the Spanner DB connection is ready.  In testing, it was observed that
    // attempting to do this would block forever if an emulator host was specified
    // with nothing listening there. Therefore, we use the async API.
    val results = singleUse().executeQuery(Statement.of("SELECT 1"))
    withTimeout(timeout.toMillis()) {
      checkNotNull(results.singleOrNull()) { "No results from Spanner ready query" }
    }
  }

  /** Non-blocking version of [com.google.cloud.spanner.ReadContext] */
  interface ReadContext : AutoCloseable {
    /** @see [com.google.cloud.spanner.ReadContext.read] */
    fun read(
      table: String,
      keys: KeySet,
      columns: Iterable<String>,
      vararg options: ReadOption
    ): Flow<Struct>

    /** @see [com.google.cloud.spanner.ReadContext.readRow] */
    suspend fun readRow(table: String, key: Key, columns: Iterable<String>): Struct?

    /** @see [com.google.cloud.spanner.ReadContext.readRowUsingIndex] */
    suspend fun readRowUsingIndex(
      table: String,
      index: String,
      key: Key,
      columns: Iterable<String>
    ): Struct?

    /** @see [com.google.cloud.spanner.ReadContext.readRowUsingIndex] */
    suspend fun readRowUsingIndex(
      table: String,
      index: String,
      key: Key,
      vararg columns: String
    ): Struct?

    /** @see [com.google.cloud.spanner.ReadContext.executeQuery] */
    fun executeQuery(statement: Statement, vararg options: QueryOption): Flow<Struct>
  }

  /** Non-blocking version of [com.google.cloud.spanner.TransactionRunner]. */
  interface TransactionRunner {
    /** @see [com.google.cloud.spanner.TransactionRunner.run] */
    suspend fun <R> execute(work: TransactionWork<R>): R

    suspend fun getCommitTimestamp(): Timestamp
  }

  /** Non-blocking version of [com.google.cloud.spanner.TransactionContext]. */
  interface TransactionContext : ReadContext {
    /** @see [com.google.cloud.spanner.TransactionContext.buffer] */
    fun buffer(mutation: Mutation)

    /** @see [com.google.cloud.spanner.TransactionContext.buffer] */
    fun buffer(mutations: Iterable<Mutation>)

    /** @see [com.google.cloud.spanner.TransactionContext.executeUpdate] */
    suspend fun executeUpdate(statement: Statement): Long
  }
}

private class ReadContextImpl(private val readContext: ReadContext) :
  AsyncDatabaseClient.ReadContext, AutoCloseable by readContext {

  override fun read(
    table: String,
    keys: KeySet,
    columns: Iterable<String>,
    vararg options: ReadOption
  ): Flow<Struct> {
    return readContext.readAsync(table, keys, columns, *options).asFlow()
  }

  override suspend fun readRow(table: String, key: Key, columns: Iterable<String>): Struct? {
    return readRowAsync(table, key, columns).await()
  }

  private fun readRowAsync(
    table: String,
    key: Key,
    columns: Iterable<String>
  ): Deferred<Struct?> {
    return readContext.readRowAsync(table, key, columns).asDeferred()
  }

  override suspend fun readRowUsingIndex(
    table: String,
    index: String,
    key: Key,
    columns: Iterable<String>
  ): Struct? {
    return readRowUsingIndexAsync(table, index, key, columns).await()
  }

  override suspend fun readRowUsingIndex(
    table: String,
    index: String,
    key: Key,
    vararg columns: String
  ): Struct? {
    return readRowUsingIndex(table, index, key, columns.asIterable())
  }

  private fun readRowUsingIndexAsync(
    table: String,
    index: String,
    key: Key,
    columns: Iterable<String>
  ): Deferred<Struct?> {
    return readContext.readRowUsingIndexAsync(table, index, key, columns).asDeferred()
  }

  override fun executeQuery(statement: Statement, vararg options: QueryOption): Flow<Struct> {
    return readContext.executeQueryAsync(statement, *options).use {
      it.asFlow()
    }
  }
}

private class TransactionRunnerImpl(private val runner: AsyncRunner) :
  AsyncDatabaseClient.TransactionRunner {

  override suspend fun <R> execute(work: TransactionWork<R>): R {
    return try {
      executeAsync(work).await()
    } catch (e: SpannerException) {
      throw e.wrappedException ?: e
    }
  }

  private fun <R> executeAsync(work: TransactionWork<R>): Deferred<R> {
    return runner.runAsync { txn ->
      ApiFutures.immediateFuture(work(TransactionContextImpl(txn)))
    }.asDeferred()
  }

  override suspend fun getCommitTimestamp(): Timestamp {
    return getCommitTimestampAsync().await()
  }

  private fun getCommitTimestampAsync(): Deferred<Timestamp> {
    return runner.commitTimestamp.asDeferred()
  }
}

private class TransactionContextImpl(private val txn: TransactionContext) :
  AsyncDatabaseClient.TransactionContext, AsyncDatabaseClient.ReadContext by ReadContextImpl(txn) {

  override fun buffer(mutation: Mutation) = txn.buffer(mutation)

  override fun buffer(mutations: Iterable<Mutation>) = txn.buffer(mutations)

  override suspend fun executeUpdate(statement: Statement): Long {
    return executeUpdateAsync(statement).await()
  }

  private fun executeUpdateAsync(statement: Statement): Deferred<Long> {
    return txn.executeUpdateAsync(statement).asDeferred()
  }
}

fun DatabaseClient.asAsync() = AsyncDatabaseClient(this)

/** Produces a [Flow] from the results in this [AsyncResultSet]. */
private fun AsyncResultSet.asFlow(): Flow<Struct> {
  val channel = Channel<Struct>()
  setCallback(executor::execute) { cursor ->
    try {
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
      when (cursor.tryNext()) {
        AsyncResultSet.CursorState.OK -> {
          channel.sendBlocking(cursor.currentRowAsStruct)
          AsyncResultSet.CallbackResponse.CONTINUE
        }
        AsyncResultSet.CursorState.NOT_READY ->
          AsyncResultSet.CallbackResponse.CONTINUE
        AsyncResultSet.CursorState.DONE -> {
          channel.close()
          AsyncResultSet.CallbackResponse.DONE
        }
      }
    } catch (e: Throwable) {
      channel.close(e)
      AsyncResultSet.CallbackResponse.DONE
    }
  }

  return channel.consumeAsFlow()
}

private fun <T> AsyncRunner.runAsync(asyncWork: AsyncWork<T>): ApiFuture<T> {
  return runAsync(
    { txn ->
      runBlocking { asyncWork(txn) }
    },
    executor::execute
  )
}

private fun <T> ApiFuture<T>.asDeferred(): Deferred<T> {
  return asDeferred(executor)
}
