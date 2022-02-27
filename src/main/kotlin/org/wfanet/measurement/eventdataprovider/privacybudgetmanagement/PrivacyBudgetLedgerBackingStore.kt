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

package org.wfanet.measurement.eventdataprovider.privacybudgetmanagement

/**
 * Representation of a single row in the privacy budget ledger backing store. Note that a given
 * PrivacyBucketGroup may have multiple rows associated to it. The total charge to the
 * PrivacyBucketGroup is obtained by aggregating all of the charges specified in all of the rows for
 * that bucket group. This aggregation may be non-linear, e.g., determining total privacy budget
 * usage is not as simple as just adding up the charges for the individual rows.
 */
data class PrivacyBudgetLedgerEntry(
  val rowId: Long,
  val privacyBucketGroup: PrivacyBucketGroup,
  val privacyCharge: PrivacyCharge,
  val repititionCount: Int
)

/** Manages the persistence of privacy budget data. */
interface PrivacyBudgetLedgerBackingStore {
  /**
   * Informs the backing store that the processing of a new requisition has commenced. All accesses
   * to the backing store between the call to startTransaction() and the final call to commit() will
   * appear as an atomic update to the database.
   *
   * @return A transaction context that can be used for subsequent interaction with the privacy
   * budget ledger backing store.
   */
  fun startTransaction(): PrivacyBudgetLedgerTransactionContext
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
 * PrivacyBudgetLedgerBackingStore should take this into account.
 */
interface PrivacyBudgetLedgerTransactionContext {
  val transactionId: Long // A unique ID assigned to this transaction.

  /**
   * Returns a list of all rows within the privacy budget ledger where the PrivacyBucket of the row
   * intersects with the given privacyBucket.
   */
  fun findIntersectingLedgerEntries(
    privacyBucketGroup: PrivacyBucketGroup
  ): List<PrivacyBudgetLedgerEntry>

  /** Adds a new row to the PrivacyBudgetLedger specifying a charge to a privacy budget. */
  fun addLedgerEntry(privacyBucketGroup: PrivacyBucketGroup, privacyCharge: PrivacyCharge)

  /** Updates a row in the PrivacyBudgetLedger. */
  fun updateLedgerEntry(privacyBudgetLedgerEntry: PrivacyBudgetLedgerEntry)

  /**
   * Causes the privacy charges from a previous request to be permanently merged into the database.
   *
   * One possible implementation is to represent the permanently merged privacy budget charges using
   * a special transaction ID (for example, 0). When merging a row, if there is an existing row with
   * the special transaction ID that has the same privacy charge, then the repetition count can be
   * increased and the merged row can be deleted. Otherwise, the transaction ID for the merged row
   * can be set to the special transaction ID.
   */
  fun mergePreviousTransaction(previousTransactionId: Long)

  /**
   * Causes the privacy charges from a previous transaction to be reversed.
   *
   * This can be implemented by deleting the rows with the previous transaction id.
   */
  fun undoPreviousTransaction(previousTransactionId: Long)

  /**
   * Commits the current transaction.
   *
   * After calling this method, it is an error to call any additional methods on this instance.
   */
  fun commit(): PrivacyBudgetManagerReturnStatus
}
