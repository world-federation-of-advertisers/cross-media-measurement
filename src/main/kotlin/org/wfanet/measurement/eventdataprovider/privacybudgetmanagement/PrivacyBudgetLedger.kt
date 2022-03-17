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

  data class ChargeWithRepetitions(val epsilon: Float, val delta: Float, val count: Int) {

    fun isEquivalentTo(other: ChargeWithRepetitions): Boolean {
      return (abs(this.epsilon - other.epsilon) < PrivacyBudgetLedgerConstants.EPSILON_EPSILON) &&
        (abs(this.delta - other.delta) < PrivacyBudgetLedgerConstants.DELTA_EPSILON)
    }
  }

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

    val nonUniqueCharges = mutableListOf<ChargeWithRepetitions>()
    for (entry in ledgerEntries) {
      nonUniqueCharges.add(
        ChargeWithRepetitions(
          entry.privacyCharge.epsilon,
          entry.privacyCharge.delta,
          entry.repetitionCount
        )
      )
    }
    for (charge in charges) {
      nonUniqueCharges.add(ChargeWithRepetitions(charge.epsilon, charge.delta, 1))
    }

    var allChargesEquivalent = true
    for (i in 1..nonUniqueCharges.size - 1) {
      if (!nonUniqueCharges[0].isEquivalentTo(nonUniqueCharges[i])) {
        allChargesEquivalent = false
        break
      }
    }

    if (allChargesEquivalent) {
      val nCharges = nonUniqueCharges.sumOf { it.count }
      val advancedCompositionEpsilon =
        totalPrivacyBudgetUsageUnderAdvancedComposition(
          PrivacyCharge(nonUniqueCharges[0].epsilon, nonUniqueCharges[0].delta),
          nCharges,
          maximumTotalDelta
        )
      val budgetExceeded =
        if (advancedCompositionEpsilon != null) advancedCompositionEpsilon > maximumTotalEpsilon
        else true
      return budgetExceeded
    } else {
      val totalEpsilon = nonUniqueCharges.sumOf { it.epsilon.toDouble() * it.count.toDouble() }
      val totalDelta = nonUniqueCharges.sumOf { it.delta.toDouble() * it.count.toDouble() }
      println("totalEpsilon = ${totalEpsilon}, totalDelta = ${totalDelta}")
      return (totalEpsilon > maximumTotalEpsilon.toDouble()) ||
        (totalDelta > maximumTotalDelta.toDouble())
    }
  }
}
