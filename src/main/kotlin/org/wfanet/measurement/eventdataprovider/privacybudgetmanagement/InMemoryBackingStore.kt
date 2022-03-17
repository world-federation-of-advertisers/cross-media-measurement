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
	val ledger: MutableList<PrivacyBudgetLedgerEntry> = mutableListOf()
	var transactionCount = 0L

	override fun startTransaction(): InMemoryBackingStoreTransactionContext {
		transactionCount += 1
		return InMemoryBackingStoreTransactionContext(ledger, transactionCount)
	}
}

class InMemoryBackingStoreTransactionContext (val ledger: MutableList<PrivacyBudgetLedgerEntry>,
override val transactionId: Long) : PrivacyBudgetLedgerTransactionContext {

	var transactionLedger = ledger.toMutableList()

	override fun findIntersectingLedgerEntries(privacyBucketGroup: PrivacyBucketGroup): List<PrivacyBudgetLedgerEntry> {
		return transactionLedger.filter {
			privacyBucketsOverlap(privacyBucketGroup, it.privacyBucketGroup)
		}
	}

	fun privacyBucketsOverlap(bucketGroup1: PrivacyBucketGroup,
														bucketGroup2: PrivacyBucketGroup): Boolean {
		if (bucketGroup1.measurementConsumerId != bucketGroup2.measurementConsumerId) {
			return false
		}
		if (bucketGroup2.endingDate.isBefore(bucketGroup1.startingDate) ||
				bucketGroup1.endingDate.isBefore(bucketGroup2.startingDate)) {
			return false
		}
		if (bucketGroup1.ageGroup != bucketGroup2.ageGroup) {
			return false
		}
		if (bucketGroup1.gender != bucketGroup2.gender) {
			return false
		}

		// The complexity of the following code illustrates why allowing
		// VID sampling buckets to wrap around was a poor design choice.
		val vidSampleEnd1 = bucketGroup1.vidSampleStart + bucketGroup1.vidSampleWidth
		val vidSampleEnd2 = bucketGroup2.vidSampleStart + bucketGroup2.vidSampleWidth
		if (vidSampleEnd1 > 1.0 && vidSampleEnd2 > 1.0) {
			// Both intervals contain 0
			return true
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

	override fun addLedgerEntry(privacyBucketGroup: PrivacyBucketGroup, privacyCharge: PrivacyCharge) {
		val ledgerEntry = PrivacyBudgetLedgerEntry(
			transactionLedger.size.toLong(),
			transactionId,
			privacyBucketGroup,
			privacyCharge,
			1)
		transactionLedger.add(ledgerEntry)
	}

	override fun updateLedgerEntry(privacyBudgetLedgerEntry: PrivacyBudgetLedgerEntry) {
		println("The ledger is ${transactionLedger}")
		transactionLedger[privacyBudgetLedgerEntry.rowId.toInt()] = privacyBudgetLedgerEntry
		println("The ledger is now ${transactionLedger}")

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
						transactionLedger[i].repetitionCount)
			}
		}
	}

	override fun undoPreviousTransaction(previousTransactionId: Long) {
		transactionLedger = transactionLedger.filter { it.transactionId != previousTransactionId }.toMutableList()
		for (i in transactionLedger.indices) {
			transactionLedger[i] =
				PrivacyBudgetLedgerEntry(
					i.toLong(),
					transactionLedger[i].transactionId,
					transactionLedger[i].privacyBucketGroup,
					transactionLedger[i].privacyCharge,
					transactionLedger[i].repetitionCount)
		}
	}

	override fun commit() {
		ledger.clear()
		ledger.addAll(transactionLedger)
	}

}
