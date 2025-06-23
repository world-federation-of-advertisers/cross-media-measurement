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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.cloud.Timestamp
import com.google.cloud.spanner.Options
import com.google.cloud.spanner.SpannerException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.logging.Logger
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient

/**
 * Abstracts a common pattern:
 * - Run a RMW transaction
 * - Optionally perform additional reads after the transaction is done
 * - Transform all of the outputs into a result
 *
 * Each SpannerWriter instance will be executed at most once.
 *
 * This provides some conveniences, like running in the right dispatcher for Spanner.
 */
abstract class SpannerWriter<T, R> {
  class TransactionScope(
    val txn: AsyncDatabaseClient.TransactionContext,
    val idGenerator: IdGenerator,
  ) {
    val transactionContext: AsyncDatabaseClient.TransactionContext
      get() = txn
  }

  data class ResultScope<T>(val transactionResult: T?, val commitTimestamp: Timestamp)

  /**
   * Override this to perform the body of the Spanner transaction.
   *
   * This uses a [TransactionScope] receiver, so it has convenient access to the
   * [AsyncDatabaseClient.TransactionContext] and [IdGenerator].
   */
  protected abstract suspend fun TransactionScope.runTransaction(): T

  /**
   * Override this to compute the final result from [execute]. This is guaranteed to run after the
   * Spanner transaction is complete.
   */
  protected abstract fun ResultScope<T>.buildResult(): R

  // To ensure the transaction is only executed once:
  private val executed = AtomicBoolean(false)

  protected val writerName = this::class.simpleName ?: "Anonymous"

  private suspend fun runTransaction(
    runner: AsyncDatabaseClient.TransactionRunner,
    idGenerator: IdGenerator,
  ): T? {
    return try {
      runner.run { txn -> TransactionScope(txn, idGenerator).runTransaction() }
    } catch (e: SpannerException) {
      handleSpannerException(e)
    }
  }

  /**
   * Executes the SpannerWriter by starting a SpannerWriter, running [runTransaction], then calling
   * [buildResult] on the output.
   *
   * This can only be called once per instance. This will bubble up anything that
   * [handleSpannerException] throws.
   *
   * @return the output of [buildResult]
   */
  suspend fun execute(databaseClient: AsyncDatabaseClient, idGenerator: IdGenerator): R {
    logger.fine("Running $writerName transaction")
    check(executed.compareAndSet(false, true)) { "Cannot execute SpannerWriter multiple times" }
    val runner = databaseClient.readWriteTransaction(Options.tag("writer=$writerName"))
    val transactionResult: T? = runTransaction(runner, idGenerator)
    val resultScope = ResultScope(transactionResult, runner.getCommitTimestamp())
    return resultScope.buildResult()
  }

  /** Override this to handle Spanner exception thrown from [execute]. */
  protected open suspend fun handleSpannerException(e: SpannerException): T? {
    throw e
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}

/** A [SpannerWriter] whose result is the non-null transaction result. */
abstract class SimpleSpannerWriter<T : Any> : SpannerWriter<T, T>() {
  final override fun ResultScope<T>.buildResult(): T {
    return checkNotNull(transactionResult)
  }
}
