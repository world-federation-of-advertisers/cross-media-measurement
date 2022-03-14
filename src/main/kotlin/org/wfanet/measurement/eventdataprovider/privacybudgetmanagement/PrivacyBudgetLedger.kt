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


/** Manages and updates privacy budget data. */
internal class PrivacyBudgetLedger(val backingStore: PrivacyBudgetLedgerBackingStore,
																	 val maximumTotalEpsilon: Float,
																	 val maximumTotalDelta: Float
) {
	/**
   *  Two differential privacy values are considered equal if they are
   *  within this amount of each other.
   */
	val EPSILON_EPSILON = 1.0E-9
	val DELTA_EPSILON = 1.0E-12

  /**
   * For each privacy bucket group in the list of PrivacyBucketGroups, adds each of the privacy
   * charges to that group.
   *
   * @throws PrivacyBudgetManagerException if the attempt to charge the privacy bucket groups was
   * unsuccessful. Possible causes could include exceeding available privacy budget or an inability
   * to commit an update to the database.
   */
  fun chargePrivacyBucketGroups(
    privacyBucketGroups: Iterable<PrivacyBucketGroup>,
    privacyCharges: Iterable<PrivacyCharge>
  ) {
		privacyBucketGroupList = List<PrivacyBucketGroup>(privacyBucketGroups)
		if (privacyBucketGroupList.size == 0) {
			return
		}

		privacyChargesList = List<PrivacyCharge>(privacyBucketCharges)
		if (privacyChargesList.size == 0) {
			return
		}

		val context = backingStore.startTransaction()
		var failedBucketList = List<PrivacyBucketGroup>()
		for (queryBucketGroup in privacyBucketGroupList) {
			val matchingLedgerEntries = context.findIntersectingLedgerEntries(queryBucketGroup)
			if (privacyBudgetIsExceeded(matchingLedgerEntries, privacyCharges)) {
				failedBucketList.append(queryBucketGroup)
			}
		}
		if (failedBucketList.size > 0) {
			context.commit()
			throw PrivacyBudgetManagerException(PRIVACY_BUDGET_EXCEEDED,
																					failedBucketList)
		}

		for (queryBucketGroup in privacyBucketGroupList) {
			val matchingLedgerEntries = context.findIntersectingLedgerEntries(queryBucketGroup)
			for (charge in privacyCharges) {
				var matchingChargeFound = False
				for (ledgerEntry in matchingLedgerEntries) {
					if (charge.equals(ledgerEntry.privacyCharge)) {
						matchingChargeFound = True
						ledgerEntry.repetitionCount += 1
						context.updateLedgerEntry(ledgerEntry)
						break
					}
				}
				if (not matchingEntryFound) {
					context.addLedgerEntry(queryBucketGroup, charge)
				}
			}
		}

		context.commit()
	}

  data class ChargeWithRepetitions(val epsilon: Float,
															val delta: Float,
															val count: Int) {

		fun isEquivalentTo(other: ChargeWithRepetitions): Boolean {
			return (abs(this.epsilon - other.epsilon) < EPSILON_EPSILON) &&
						 (abs(this.delta - other.delta) < DELTA_EPSILON)
		}

	}

  fun privacyBudgetIsExceeded(ledgerEntries: List<PrivacyBudgetLedgerEntry>,
															charges: List<PrivacyBucketCharges>): Boolean {
		val nonUniqueCharges = List<ChargeWithRepetitions>
		for (entry in ledgerEntries) {
			nonUniqueCharges.add(ChargeWithRepetitions(entry.privacyCharge.epsilon,
													 entry.privacyCharge.delta, entry.repetitionCount))
		}
		for (charge in Charges) {
			nonUniqueCharges.add(charge.epsilon, charge.delta, 1)
		}

		var allChargesEquivalent = True
		for (i = 1; i < nonUniqueCharges.size; i++) {
			if (!nonUniqueCharges[0].isEquivalentTo(nonUniqueCharges[i])) {
				allChargesEquivalent = False
				break
			}
		}

	  if (allChargesEquivalent) {
			val nCharges = nonUniqueCharges.sumOf { it.count }
			val advancedCompositionEpsilon =
				totalPrivacyBudgetUsageUnderAdvancedComposition(
					nonUniqueCharges[0].epsilon, nCharges, maximumTotalDelta)
			return advancedCompositionEpsilon > maximumTotalEpsilon
		} else {
			val totalEpsilon = nonUniqueCharges.sumOf { it.epsilon * it.count.toFloat() }
			val totalDelta = nonUniqueCharges.sumOf { it.delta * it.count.toFloat() }
			return (totalEpsilon > maximumTotalEpsilon) || (totalDelta > maximumTotalDelta)
		}
	}

}
