/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.wfanet.measurement.privacybudgetmanager

private const val MAX_BATCH_INSERT = 1000
private const val MAX_BATCH_READ = 1000

/**
 * Manages interaction with the persistance layer that stores [PrivacyBucket]s, [Charge]s and
 * [Query]s
 */
interface Ledger {
  /**
   * Informs the backing store that the processing of new requisitions has commenced. All accesses
   * to the backing store between the call to startTransaction() and the final call to commit() will
   * appear as an atomic update to the database.
   *
   * @return A transaction context that can be used for subsequent interaction with the privacy
   *   budget backing store.
   */
  fun startTransaction(): TransactionContext
}

/**
 * Manages accesses to the privacy budget ledger in the context of a single transaction.
 *
 * A primary purpose of the TransactionContext is to guarantee ACID properties for the underlying
 * data store.
 *
 * The privacy budget manager will not use more than one thread per requisition, however it is
 * possible that several requisitions may be in process simultaneously by different threads. Also,
 * if the privacy budget manager is replicated across nodes, it is conceivable that different nodes
 * could be processing privacy budget management operations simultaneously. Implementors of the
 * BackingStore should take this into account. Since this interface is AutoCloseable, when the
 * context is closed without explicitly calling commit(), the expected behavior of the
 * implementation is to rollback the transaction.
 */
interface TransactionContext : AutoCloseable {
  /**
   * Reads and returns the Queries that are already present with their CreateTime read from the DB.
   *
   * @param list of queries to find in the ledger.
   * @returns list of [Query] that existed in the ledger. These have createTime field populated.
   * @throws PrivacyBudgetManager exception if the read operation was unsuccessful.
   */
  suspend fun readQueries(queries: List<Query>, maxBatchSize: Int = MAX_BATCH_READ): List<Query>

  /**
   * Reads the rows specified by the [rowKeys] from the db.
   *
   * @throws PrivacyBudgetManager exception if the read operation was unsuccessful.
   */
  suspend fun readChargeRows(rowKeys: List<LedgerRowKey>, maxBatchSize: Int = MAX_BATCH_READ): Slice

  /**
   * Writes a [Slice] to the backing store together with the [queries] responsible for its creation.
   *
   * @returns the Queries that are written with their createTime read from the DB.
   * @throws PrivacyBudgetManager exception if the write operation was unsuccessful.
   */
  suspend fun write(
    delta: Slice,
    queries: List<Query>,
    maxBatchSize: Int = MAX_BATCH_INSERT,
  ): List<Query>

  /**
   * Commits the current transaction.
   *
   * After calling this method, it is an error to call any additional methods on this instance.
   *
   * @throws PrivacyBudgetManager exception if the commit operation was unsuccessful.
   */
  suspend fun commit()
}
