// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.deploy.postgres.writers

import io.r2dbc.spi.Connection
import java.util.concurrent.atomic.AtomicBoolean
import java.util.logging.Logger
import org.wfanet.measurement.common.identity.IdGenerator
import reactor.core.publisher.Mono

/** Abstraction for writing to Postgres. */
abstract class PostgresWriter<T> {
  data class TransactionScope(val connection: Connection, val idGenerator: IdGenerator)

  /**
   * Override this to perform the body of the Postgres transaction.
   *
   * This runs in the scope of a [TransactionScope], so it has convenient access to the [Connection]
   * and [IdGenerator].
   */
  protected abstract suspend fun TransactionScope.runTransaction(): T

  // To ensure the transaction is only executed once:
  private val executed = AtomicBoolean(false)

  private suspend fun runTransaction(connection: Connection, idGenerator: IdGenerator): Mono<T> {
    return try {
      connection.beginTransaction()
      val scope = TransactionScope(connection, idGenerator)
      val transactionResult = scope.runTransaction()
      connection.commitTransaction()
      Mono.just(transactionResult)
    } catch (e1: Exception) {
      try {
        connection.rollbackTransaction()
        Mono.error(e1)
      } catch (e2: Exception) {
        Mono.error(e2)
      }
    }
  }

  /**
   * Executes the PostgresWriter by starting a PostgresWriter then running [runTransaction].
   *
   * This can only be called once per instance.
   *
   * @return the output of [runTransaction]
   */
  suspend fun execute(connection: Connection, idGenerator: IdGenerator): Mono<T> {
    logger.fine("Running ${this::class.simpleName} transaction")
    check(executed.compareAndSet(false, true)) { "Cannot execute PostgresWriter multiple times" }
    return runTransaction(connection, idGenerator)
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
