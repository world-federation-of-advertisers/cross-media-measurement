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
package org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.testing

import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBucketGroup
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetLedgerBackingStore
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetLedgerEntry
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetLedgerTransactionContext
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyCharge

/**
 * A simple implementation of a privacy budget ledger backing store.
 *
 * The purpose of this class is to facilitate implementation of unit tests of the privacy budget
 * ledger. Also, hopefully this can serve as a guide for implementors of more sophisticated backing
 * stores. This code is not thread safe.
 */
class InMemoryBackingStore : PrivacyBudgetLedgerBackingStore {
  private val ledger: MutableList<PrivacyBudgetLedgerEntry> = mutableListOf()
  private var transactionCount = 0L

  override fun startTransaction(): InMemoryBackingStoreTransactionContext {
    transactionCount += 1
    return InMemoryBackingStoreTransactionContext(ledger, transactionCount)
  }

  override fun close() {}
}

class InMemoryBackingStoreTransactionContext(
  val ledger: MutableList<PrivacyBudgetLedgerEntry>,
  override val transactionId: Long,
) : PrivacyBudgetLedgerTransactionContext {

  private var transactionLedger = ledger.toMutableList()

  override fun findIntersectingLedgerEntries(
    privacyBucketGroup: PrivacyBucketGroup,
  ): List<PrivacyBudgetLedgerEntry> {
    return transactionLedger.filter { privacyBucketGroup.overlapsWith(it.privacyBucketGroup) }
  }

  override fun addLedgerEntry(
    privacyBucketGroup: PrivacyBucketGroup,
    privacyCharge: PrivacyCharge,
  ) {
    val ledgerEntry =
      PrivacyBudgetLedgerEntry(
        transactionLedger.size.toLong(),
        transactionId,
        privacyBucketGroup,
        privacyCharge,
        1
      )
    transactionLedger.add(ledgerEntry)
  }

  override fun updateLedgerEntry(privacyBudgetLedgerEntry: PrivacyBudgetLedgerEntry) {
    transactionLedger[privacyBudgetLedgerEntry.rowId.toInt()] = privacyBudgetLedgerEntry
  }

  override fun mergePreviousTransaction(previousTransactionId: Long) {
    for (i in transactionLedger.indices) {
      if (transactionLedger[i].transactionId == previousTransactionId) {
        transactionLedger[i] =
          PrivacyBudgetLedgerEntry(
            transactionLedger[i].rowId,
            0L,
            transactionLedger[i].privacyBucketGroup,
            transactionLedger[i].privacyCharge,
            transactionLedger[i].repetitionCount
          )
      }
    }
  }

  override fun undoPreviousTransaction(previousTransactionId: Long) {
    transactionLedger =
      transactionLedger.filter { it.transactionId != previousTransactionId }.toMutableList()
    for (i in transactionLedger.indices) {
      transactionLedger[i] =
        PrivacyBudgetLedgerEntry(
          i.toLong(),
          transactionLedger[i].transactionId,
          transactionLedger[i].privacyBucketGroup,
          transactionLedger[i].privacyCharge,
          transactionLedger[i].repetitionCount
        )
    }
  }

  override fun commit() {
    ledger.clear()
    ledger.addAll(transactionLedger)
  }

  override fun close() {}
}
