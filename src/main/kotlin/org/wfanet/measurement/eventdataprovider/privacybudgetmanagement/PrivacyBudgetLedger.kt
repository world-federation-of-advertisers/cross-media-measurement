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

import kotlin.math.abs

object PrivacyBudgetLedgerConstants {
  /**
   * Two differential privacy values are considered equal if they are within this amount of each
   * other.
   */
  const val EPSILON_EPSILON = 1.0E-9
  const val DELTA_EPSILON = 1.0E-12
}

data class ChargeWithRepetitions(val epsilon: Float, val delta: Float, val count: Int)

fun ChargeWithRepetitions.isEquivalentTo(other: ChargeWithRepetitions): Boolean =
  (abs(this.epsilon - other.epsilon) < PrivacyBudgetLedgerConstants.EPSILON_EPSILON) &&
    (abs(this.delta - other.delta) < PrivacyBudgetLedgerConstants.DELTA_EPSILON)

/** Manages and updates privacy budget data. */
class PrivacyBudgetLedger(
  val backingStore: PrivacyBudgetLedgerBackingStore,
  val maximumTotalEpsilon: Float,
  val maximumTotalDelta: Float
) {

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
    val privacyBucketGroupList = privacyBucketGroups.toList()
    if (privacyBucketGroupList.size == 0) {
      return
    }

    val privacyChargesList = privacyCharges.toList()
    if (privacyChargesList.size == 0) {
      return
    }

    val context = backingStore.startTransaction()
    val failedBucketList = mutableListOf<PrivacyBucketGroup>()
    for (queryBucketGroup in privacyBucketGroupList) {
      val matchingLedgerEntries = context.findIntersectingLedgerEntries(queryBucketGroup)
      if (privacyBudgetIsExceeded(matchingLedgerEntries, privacyChargesList)) {
        failedBucketList.add(queryBucketGroup)
      }
    }
    if (failedBucketList.size > 0) {
      context.commit()
      throw PrivacyBudgetManagerException(
        PrivacyBudgetManagerExceptionType.PRIVACY_BUDGET_EXCEEDED,
        failedBucketList
      )
    }

    for (queryBucketGroup in privacyBucketGroupList) {
      val matchingLedgerEntries = context.findIntersectingLedgerEntries(queryBucketGroup)
      for (charge in privacyCharges) {
        var matchingChargeFound = false
        for (ledgerEntry in matchingLedgerEntries) {
          if (charge.equals(ledgerEntry.privacyCharge)) {
            matchingChargeFound = true
            val newLedgerEntry =
              PrivacyBudgetLedgerEntry(
                ledgerEntry.rowId,
                ledgerEntry.transactionId,
                ledgerEntry.privacyBucketGroup,
                ledgerEntry.privacyCharge,
                ledgerEntry.repetitionCount + 1
              )
            context.updateLedgerEntry(newLedgerEntry)
            break
          }
        }
        if (!matchingChargeFound) {
          context.addLedgerEntry(queryBucketGroup, charge)
        }
      }
    }

    context.commit()
  }

  private fun exceedsUnderAdvancedComposition(
    charges: List<ChargeWithRepetitions>,
    maximumTotalDelta: Float,
    maximumTotalEpsilon: Float
  ): Boolean {
    val advancedCompositionEpsilon =
      Composition.totalPrivacyBudgetUsageUnderAdvancedComposition(
        PrivacyCharge(charges[0].epsilon, charges[0].delta),
        charges.sumOf { it.count },
        maximumTotalDelta
      )
    return if (advancedCompositionEpsilon != null) advancedCompositionEpsilon > maximumTotalEpsilon
    else true
  }

  fun exceedsUnderSimpleComposition(
    charges: List<ChargeWithRepetitions>,
    maximumTotalDelta: Float,
    maximumTotalEpsilon: Float
  ) =
    (charges.sumOf { it.epsilon.toDouble() * it.count.toDouble() } >
      maximumTotalEpsilon.toDouble()) ||
      (charges.sumOf { it.delta.toDouble() * it.count.toDouble() } > maximumTotalDelta.toDouble())

  /**
   * Tests whether a given privacy bucket is exceeded.
   *
   * @param ledgerEntries is a list of PrivacyBudgetLedgerEntries that all refer to the same
   * underlying privacy bucket.
   * @param charges is a list of charges that are to be added to this privacy bucket.
   * @return true if adding the charges would cause the total privacy budget usage for this bucket
   * to be exceeded.
   */
  private fun privacyBudgetIsExceeded(
    ledgerEntries: List<PrivacyBudgetLedgerEntry>,
    charges: List<PrivacyCharge>
  ): Boolean {

    val allCharges: List<ChargeWithRepetitions> =
      charges.map { ChargeWithRepetitions(it.epsilon, it.delta, 1) } +
        ledgerEntries.map {
          ChargeWithRepetitions(
            it.privacyCharge.epsilon,
            it.privacyCharge.delta,
            it.repetitionCount
          )
        }

    // We can save some privacy budget by using the advanced composition theorem if
    // all the charges are equivalent.
    return if (allCharges.all { allCharges[0].isEquivalentTo(it) })
      exceedsUnderAdvancedComposition(allCharges, maximumTotalDelta, maximumTotalEpsilon)
    else exceedsUnderSimpleComposition(allCharges, maximumTotalDelta, maximumTotalEpsilon)
  }
}
