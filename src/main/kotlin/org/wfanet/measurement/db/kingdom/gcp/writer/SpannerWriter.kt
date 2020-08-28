package org.wfanet.measurement.db.kingdom.gcp.writer

import com.google.cloud.Timestamp
import com.google.cloud.spanner.DatabaseClient
import com.google.cloud.spanner.TransactionContext
import java.time.Clock
import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.db.gcp.spannerDispatcher

/**
 * Abstracts a common pattern:
 *  - Run a RMW transaction
 *  - Optionally perform additional reads after the transaction is done
 *  - Transform all of the outputs into a result
 *
 * Each SpannerWriter instance will be executed at most once.
 *
 * This provides some conveniences, like running in the right dispatcher for Spanner.
 */
abstract class SpannerWriter<T, R> {
  data class TransactionScope(
    val transactionContext: TransactionContext,
    val idGenerator: IdGenerator,
    val clock: Clock
  )

  data class ResultScope<T>(
    val transactionResult: T?,
    val commitTimestamp: Timestamp
  )

  /**
   * Override this to perform the body of the Spanner transaction.
   *
   * This runs in the scope of a [TransactionScope], so it has convenient access to the
   * [TransactionContext], an [IdGenerator], and a [Clock].
   */
  protected abstract suspend fun TransactionScope.runTransaction(): T

  /**
   * Override this to compute the final result from [execute]. This is guaranteed to run after the
   * Spanner transaction is complete.
   */
  protected abstract fun ResultScope<T>.computeResult(): R

  // To ensure the transaction is only executed once:
  private val executed = AtomicBoolean(false)

  /**
   * Executes the SpannerWriter by starting a SpannerWriter, running [runTransaction], then calling
   * [computeResult] on the output.
   *
   * This can only be called once per instance.
   *
   * @return the output of [computeResult]
   */
  fun execute(
    databaseClient: DatabaseClient,
    idGenerator: IdGenerator,
    clock: Clock
  ): R {
    check(executed.compareAndSet(false, true)) { "Cannot execute SpannerWriter multiple times" }
    val runner = databaseClient.readWriteTransaction()
    val transactionResult: T? = runner.run { transactionContext ->
      val scope = TransactionScope(transactionContext, idGenerator, clock)
      runBlocking(spannerDispatcher()) {
        scope.runTransaction()
      }
    }
    val resultScope = ResultScope(transactionResult, runner.commitTimestamp)
    return runBlocking(spannerDispatcher()) { resultScope.computeResult() }
  }
}
