/**
 * Copyright 2022 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * ```
 *      http://www.apache.org/licenses/LICENSE-2.0
 * ```
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.wfanet.measurement.eventdataprovider.privacybudgetmanagement

import java.time.Instant

/**
 * Representation of a single row in the privacy budget ledger backing store. Note that a given
 * PrivacyBucketGroup may have multiple rows associated to it. The total charge to the
 * PrivacyBucketGroup is obtained by aggregating all of the charges specified in all of the rows for
 * that bucket group. This aggregation may be non-linear, e.g., determining total privacy budget
 * usage is not as simple as just adding up the charges for the individual rows.
 */
data class PrivacyBudgetLedgerEntry(
  val privacyBucketGroup: PrivacyBucketGroup,
  val privacyCharge: PrivacyCharge,
  val repetitionCount: Int
)

data class PrivacyBudgetReferenceEntry(
  val referenceKey: String,
  val isRefund: Boolean,
  val createTime: Instant
)

/** Manages the persistence of privacy budget data. */
interface PrivacyBudgetLedgerBackingStore : AutoCloseable {
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
interface PrivacyBudgetLedgerTransactionContext : AutoCloseable {

  /**
   * Returns a list of all rows within the privacy budget ledger where the PrivacyBucket of the row
   * intersects with the given privacyBucket.
   */
  fun findIntersectingLedgerEntries(
    privacyBucketGroup: PrivacyBucketGroup
  ): List<PrivacyBudgetLedgerEntry>

  /**
   * Adds new rows to the PrivacyBudgetLedger specifying a charge to a privacy budget, adds the
   * privacyReference that created these charges
   */
  fun addLedgerEntries(
    privacyBucketGroups: Set<PrivacyBucketGroup>,
    privacyCharges: Set<PrivacyCharge>,
    privacyReference: PrivacyReference
  )

  /** Checks if the charges with the [referenceKey] should be processed. */
  fun shouldProcess(referenceKey: String, isRefund: Boolean): Boolean

  // TODO(@uakyol) : expose reference entries for replayability purposes.

  /**
   * Commits the current transaction.
   *
   * After calling this method, it is an error to call any additional methods on this instance.
   *
   * @throws PrivacyBudgetManager exception if the commit operation was unsuccessful.
   */
  fun commit()
}
