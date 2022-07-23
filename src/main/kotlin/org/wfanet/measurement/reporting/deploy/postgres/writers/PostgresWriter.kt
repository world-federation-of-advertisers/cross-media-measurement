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

import java.util.concurrent.atomic.AtomicBoolean
import java.util.logging.Logger
import org.wfanet.measurement.common.db.r2dbc.DatabaseClient
import org.wfanet.measurement.common.db.r2dbc.ReadWriteContext
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.reporting.service.internal.ReportingInternalException

/** Abstraction for writing to Postgres. */
abstract class PostgresWriter<T> {
  data class TransactionScope(
    val transactionContext: ReadWriteContext,
    val idGenerator: IdGenerator
  )

  /**
   * Override this to perform the body of the Postgres transaction.
   *
   * This runs in the scope of a [TransactionScope], so it has convenient access to the
   * [ReadWriteContext] and [IdGenerator].
   */
  protected abstract suspend fun TransactionScope.runTransaction(): T

  // To ensure the transaction is only executed once:
  private val executed = AtomicBoolean(false)

  private suspend fun runTransaction(
    transactionContext: ReadWriteContext,
    idGenerator: IdGenerator
  ): T {
    val result: T
    try {
      val scope = TransactionScope(transactionContext, idGenerator)
      result = scope.runTransaction()
      transactionContext.commit()
    } finally {
      transactionContext.close()
    }
    return result
  }

  /**
   * Executes the PostgresWriter by starting a PostgresWriter then running [runTransaction].
   *
   * This can only be called once per instance.
   *
   * @return the output of [runTransaction]
   * @throws [ReportingInternalException] on failure. See [PostgresWriter] subclass for specifics.
   */
  suspend fun execute(databaseClient: DatabaseClient, idGenerator: IdGenerator): T {
    logger.fine("Running ${this::class.simpleName} transaction")
    check(executed.compareAndSet(false, true)) { "Cannot execute PostgresWriter multiple times" }
    val transactionContext = databaseClient.readWriteTransaction()
    return runTransaction(transactionContext, idGenerator)
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
