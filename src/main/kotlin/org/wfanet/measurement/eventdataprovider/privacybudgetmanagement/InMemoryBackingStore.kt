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
 * The purpose of this class is to facilitate implementation of unit
 * tests of the privacy budget ledger.  Also, hopefully this can serve
 * as a guide for implementors of more sophisticated backing stores.
 * This code is not thread safe.
 */
class InMemoryBackingStore: PrivacyBudgetLedgerBackingStore {
	val ledger = List<PrivacyBudgetLedgerEntry>()
	var transactionCount = 0

	override fun startTransaction() {
		transactionCount += 1
		return InMemoryPrivacyBackingStoreTransactionContext(ledger, transactionCount)
	}
}

class InMemoryBackingStoreTransactionContext (val ledger: List<PrivacyBudgetLedgerEntry>,
val transactionId: Int) : PrivacyBudgetLedgerTransactionContext {

	var transactionLedger = List<PrivacyBudgetLedgerEntry>(ledger)

	fun findIntersectingLedgerEntries(privacyBucketGroup: PrivacyBucketGroup) {
		return transactionLedger.filter {
			privacyBucketsOverlap(privacyBucketGroup, it.privacyBucketGroup)
		}
	}

	fun privacyBucketsOverlap(bucketGroup1: PrivacyBucketGroup,
														bucketGroup2: PrivacyBucketGroup): Boolean {
		if (bucketGroup2.endingDate.isBefore(bucketGroup1.startingDate) ||
				bucketGroup1.endingDate.isBefore(bucketGroup2.startingDate)) {
			return False
		}
		if (bucketGroup1.ageGroup != bucketGroup2.ageGroup) {
			return False
		}
		if (bucketGroup1.gender != bucketGroup2.gender) {
			return False
		}

		// The complexity of the following code illustrates why allowing
		// VID sampling buckets to wrap around was a poor design choice.
		val vidSampleEnd1 = bucketGroup1.vidSampleStart + bucketGroup1.vidSampleWidth
		val vidSampleEnd2 = bucketGroup2.vidSampleStart + bucketGroup2.vidSampleWidth
		if (vidSampleEnd1 > 1.0 && vidSampleEnd2 > 1.0) {
			// Both intervals contain 0
			return True
		}

		if (vidSampleEnd1 > 1.0) {
			return !((vidSampleEnd1 - 1.0 < bucketGroup2.vidSampleStart) &&
								(vidSampleEnd2 < bucketGroup1.vidSampleStart))
		}

		if (vidSampleEnd2 > 1.0) {
			return !((vidSampleEnd2 - 1.0 < bucketGroup1.vidSampleStart) &&
								(vidSampleEnd1 < bucketGroup2.vidSampleStart))
		}

		return (bucketGroup1.vidSampleStart <= vidSampleEnd2) &&
  		(bucketGroup2.vidSampleStart <= vidSampleEnd1)
	}

	fun addLedgerEntry(privacyBucketGroup: PrivacyBucketGroup, privacyCharge: PrivacyCharge) {
		val ledgerEntry = PrivacyBucketLedgerEntry(
			transactionLedger.size,
			transactionId,
			privacyBucketGroup,
			privacyCharge,
			1)
		transactionLedger.add(ledgerEntry)
	}

	fun updateLedgerEntry(privacyBudgetLedgerEntry: PrivacyBudgetLedgerEntry) {
		transactionLedger[privacyBudgetLedgerEntry.rowId] = privacyBudgetLedgerEntry
	}

	fun mergePreviousTransaction(previousTransactionId: Long) {
		transactionLedger.forEach {
			if(it.transactionId == previousTransactionId) {
				it.transactionId = 0
			}
		}
	}

	fun undoPreviousTransaction(previousTransactionId: Long) {
		transactionLedger = transactionLedger.filter { it.transactionId != previousTransactionId }
		transactionLedger.forEachIndexed { i, entry -> entry.rowId = i }
	}

	fun commit() {
		ledger.clear()
		ledger.addAll(transactionLedger)
	}

}
