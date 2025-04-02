/*
 * Copyright 2022 The Cross-Media Measurement Authors
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
package org.wfanet.measurement.eventdataprovider.privacybudgetmanagement

import java.time.Instant

/**
 * Representation of an AcdpCharge balance for [privacyBucketGroup] in the privacy budget ledger
 * backing store. Note that a given [privacyBucketGroup] should only have one row associated to it
 * which is the aggregated [acdpCharge]s. When checking the privacy usage, the aggregated
 * [acdpCharge] is first converted to Delta in Differential Privacy Params(Epsilon, Delta) and check
 * against the maximum Delta set for each [privacyBucketGroup].
 */
data class PrivacyBudgetAcdpBalanceEntry(
  val privacyBucketGroup: PrivacyBucketGroup,
  val acdpCharge: AcdpCharge,
)

/**
 * Representation of a single query that resulted in multiple charges in the privacy budget ledger
 * backing store. These entries only exists for replays, and is a list of timestamped transactions.
 */
data class PrivacyBudgetLedgerEntry(
  val measurementConsumerId: String,
  val referenceId: String,
  val isRefund: Boolean,
  val createTime: Instant,
)

/** Manages the persistence of privacy budget data. */
interface PrivacyBudgetLedgerBackingStore : AutoCloseable {
  /**
   * Informs the backing store that the processing of a new requisition has commenced. All accesses
   * to the backing store between the call to startTransaction() and the final call to commit() will
   * appear as an atomic update to the database.
   *
   * @return A transaction context that can be used for subsequent interaction with the privacy
   *   budget ledger backing store.
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
   * Returns the row in the PrivacyBudgetAcdpBalance of the PrivacyBucket. Each PrivacyBucketGroup
   * should only have one row.
   */
  suspend fun findAcdpBalanceEntry(
    privacyBucketGroup: PrivacyBucketGroup
  ): PrivacyBudgetAcdpBalanceEntry

  /**
   * Returns a set of exceeded balance entries. For each PrivacyBucketGroup, one
   * PrivacyBudgetAcdpBalanceEntry should be generated.
   *
   * Override this method to increase the performance by batching search for PrivacyBucketGroup,
   * instead of looking for each entry individually. The default implementation calls the
   * findAcdpBalanceEntry method individually.
   */
  open suspend fun findAcdpBalanceEntries(
    privacyBucketGroup: Set<PrivacyBucketGroup>
  ): Set<PrivacyBudgetAcdpBalanceEntry> =
    privacyBucketGroup.map { findAcdpBalanceEntry(it) }.toSet()

  /**
   * Adds new entries to the PrivacyBudgetAcdpLedger specifying a AcdpCharge to a privacy budget,
   * adds the [reference] that created these charges
   */
  suspend fun addAcdpLedgerEntries(
    privacyBucketGroups: Set<PrivacyBucketGroup>,
    acdpCharges: Set<AcdpCharge>,
    reference: Reference,
  )

  fun getQueryTotalAcdpCharge(acdpCharges: Set<AcdpCharge>, isRefund: Boolean): AcdpCharge {
    // If reference is refund, subtract the acdpCharge balance.
    val totalRho: Double =
      if (isRefund) -acdpCharges.sumOf { it.rho } else acdpCharges.sumOf { it.rho }
    val totalTheta: Double =
      if (isRefund) -acdpCharges.sumOf { it.theta } else acdpCharges.sumOf { it.theta }

    return AcdpCharge(totalRho, totalTheta)
  }

  /**
   * Returns whether this backing store has a ledger entry for [reference].
   *
   * See if there's an existing ledger entry by assuming that the timestamp is either
   * 1. that of the most recent entry if the most recent entry with (MC ID, Reference ID) also has
   *    the same value for isRefund, or
   * 2. now. This can return an inaccurate result if having multiple in-flight entries with the same
   *    (MC ID, reference ID). This is because we only check that isRefund is the opposite of the
   *    most recent recorded ledger entry for that tuple.
   */
  suspend fun hasLedgerEntry(reference: Reference): Boolean

  // TODO(@uakyol) : expose reference entries for replayability purposes.

  /**
   * Commits the current transaction.
   *
   * After calling this method, it is an error to call any additional methods on this instance.
   *
   * @throws PrivacyBudgetManager exception if the commit operation was unsuccessful.
   */
  suspend fun commit()
}
