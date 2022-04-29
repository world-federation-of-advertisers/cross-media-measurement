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

/**
 * A simple implementation of a privacy budget ledger backing store.
 *
 * The purpose of this class is to facilitate implementation of unit tests of the privacy budget
 * ledger. Also, hopefully this can serve as a guide for implementors of more sophisticated backing
 * stores. This code is not thread safe.
 */
class InMemoryBackingStore : PrivacyBudgetLedgerBackingStore {

  val ledger: HashMap<PrivacyBucketGroup, MutableList<PrivacyBudgetLedgerEntry>> = hashMapOf()
  // val ledger: MutableList<PrivacyBudgetLedgerEntry> = mutableListOf()
  private var transactionCount = 0L

  override fun startTransaction(): InMemoryBackingStoreTransactionContext {
    transactionCount += 1
    return InMemoryBackingStoreTransactionContext(ledger, transactionCount)
  }

  override fun close() {}
}

class InMemoryBackingStoreTransactionContext(
  val ledger: HashMap<PrivacyBucketGroup, MutableList<PrivacyBudgetLedgerEntry>>,
  override val transactionId: Long,
) : PrivacyBudgetLedgerTransactionContext {

  private var transactionLedger = ledger.toMutableMap()

  override fun findIntersectingLedgerEntries(
    privacyBucketGroup: PrivacyBucketGroup,
  ): List<PrivacyBudgetLedgerEntry> {
    return transactionLedger.getOrDefault(privacyBucketGroup, listOf())
    // .filter { privacyBucketGroup.overlapsWith(it.privacyBucketGroup) }
  }

  override fun addLedgerEntry(
    privacyBucketGroup: PrivacyBucketGroup,
    privacyCharge: PrivacyCharge,
  ) {
    // println(transactionLedger.size)
    val ledgerEntries = transactionLedger.getOrDefault(privacyBucketGroup, mutableListOf())
    // println(ledgerEntries.size)
    if (ledgerEntries.isEmpty()) {
      ledgerEntries.add(
        PrivacyBudgetLedgerEntry(
          transactionLedger.size.toLong(),
          transactionId,
          privacyBucketGroup,
          privacyCharge,
          1
        )
      )
      transactionLedger.put(privacyBucketGroup, ledgerEntries)
      return
    }
    if (ledgerEntries.size > 1) {
      throw PrivacyBudgetManagerException(
        PrivacyBudgetManagerExceptionType.INVALID_BACKING_STORE_STATE,
        ledgerEntries.map { privacyBucketGroup }
      )
    }

    ledgerEntries.forEachIndexed { index, element ->
      if (element.privacyCharge.equals(privacyCharge)) {
        ledgerEntries[index] =
          PrivacyBudgetLedgerEntry(
            element.rowId,
            element.transactionId,
            element.privacyBucketGroup,
            element.privacyCharge,
            element.repetitionCount + 1
          )
      }
    }
  }

  override fun updateLedgerEntry(privacyBudgetLedgerEntry: PrivacyBudgetLedgerEntry) = TODO("TODO(@uakyol) implement this")

  override fun mergePreviousTransaction(previousTransactionId: Long)= TODO("TODO(@uakyol) implement this")

  override fun undoPreviousTransaction(previousTransactionId: Long)  = TODO("TODO(@uakyol) implement this")
  //     {
  //   transactionLedger =
  //     transactionLedger.filter { it.transactionId != previousTransactionId }.toMutableList()
  //   for (i in transactionLedger.indices) {
  //     transactionLedger[i] =
  //       PrivacyBudgetLedgerEntry(
  //         i.toLong(),
  //         transactionLedger[i].transactionId,
  //         transactionLedger[i].privacyBucketGroup,
  //         transactionLedger[i].privacyCharge,
  //         transactionLedger[i].repetitionCount
  //       )
  //   }
  // }

  override fun commit() {
    ledger.clear()
    ledger.putAll(transactionLedger)
  }

  override fun close() {}
}
