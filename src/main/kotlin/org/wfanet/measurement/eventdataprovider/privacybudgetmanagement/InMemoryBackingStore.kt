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
 * A simple implementation of a privacy budget ledger backing store.
 *
 * The purpose of this class is to facilitate implementation of unit tests of the privacy budget
 * ledger. Also, hopefully this can serve as a guide for implementors of more sophisticated backing
 * stores. This code is not thread safe.
 *
 * This Backing store simple and fast.It is small enough to fit in memory. Thus, can be a good fit
 * for use cases such as:
 *
 * 1) Privacy Budget Management for small number of Measurement Consumers (<10).
 *
 * 2) Feeding all of the charges in a single run such as estimating total consumption for a known
 * set of queries.
 *
 * 3) Where multiple tasks are not expected to update it.
 */
open class InMemoryBackingStore : PrivacyBudgetLedgerBackingStore {
  protected val balances:
    MutableMap<PrivacyBucketGroup, MutableMap<Charge, PrivacyBudgetBalanceEntry>> =
    mutableMapOf()
  private val referenceLedger: MutableList<PrivacyBudgetLedgerEntry> = mutableListOf()

  override fun startTransaction(): InMemoryBackingStoreTransactionContext =
    InMemoryBackingStoreTransactionContext(balances, referenceLedger)

  override fun close() {}
}

class InMemoryBackingStoreTransactionContext(
  val balances:
    MutableMap<PrivacyBucketGroup, MutableMap<Charge, PrivacyBudgetBalanceEntry>>,
  val referenceLedger: MutableList<PrivacyBudgetLedgerEntry>,
) : PrivacyBudgetLedgerTransactionContext {

  private var transactionBalances = balances.toMutableMap()
  private var transactionReferenceLedger = referenceLedger.toMutableList()

  // Adds a new row to the ledger referencing an element that caused charges to the store this key
  // is usually the requisitionId.
  private fun addReferenceEntry(privacyReference: Reference) {
    transactionReferenceLedger.add(
      PrivacyBudgetLedgerEntry(
        privacyReference.measurementConsumerId,
        privacyReference.referenceId,
        privacyReference.isRefund,
        Instant.now()
      )
    )
  }

  override fun shouldProcess(referenceId: String, isRefund: Boolean): Boolean =
    transactionReferenceLedger
      .filter { it.referenceId == referenceId }
      .sortedByDescending { it.createTime }
      .firstOrNull()
      ?.isRefund?.xor(isRefund)
      ?: true

  override fun findIntersectingLedgerEntries(
    privacyBucketGroup: PrivacyBucketGroup,
  ): List<PrivacyBudgetBalanceEntry> {
    return transactionBalances.getOrDefault(privacyBucketGroup, mapOf()).values.toList()
  }

  override fun addLedgerEntries(
    privacyBucketGroups: Set<PrivacyBucketGroup>,
    Charges: Set<Charge>,
    privacyReference: Reference
  ) {
    // Update the balance for all the charges.
    for (queryBucketGroup in privacyBucketGroups) {
      for (charge in Charges) {
        val balanceEntries = transactionBalances.getOrPut(queryBucketGroup) { mutableMapOf() }

        val balanceEntry =
          balanceEntries.getOrPut(charge) { PrivacyBudgetBalanceEntry(queryBucketGroup, charge, 0) }
        balanceEntries.put(
          charge,
          PrivacyBudgetBalanceEntry(
            queryBucketGroup,
            charge,
            if (privacyReference.isRefund) balanceEntry.repetitionCount - 1
            else balanceEntry.repetitionCount + 1
          )
        )
      }
    }

    // Record the reference for these charges.
    addReferenceEntry(privacyReference)
  }

  override fun commit() {
    referenceLedger.clear()
    referenceLedger.addAll(transactionReferenceLedger)

    balances.clear()
    balances.putAll(transactionBalances)
  }

  override fun close() {}
}
