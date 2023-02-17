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

private fun ChargeWithRepetitions.totalEpsilon(): Double = epsilon.toDouble() * count

private fun ChargeWithRepetitions.totalDelta(): Double = delta.toDouble() * count

private fun ChargeWithRepetitions.isEquivalentTo(other: ChargeWithRepetitions): Boolean =
  this.epsilon.approximatelyEqualTo(other.epsilon, PrivacyBudgetLedgerConstants.EPSILON_EPSILON) &&
    this.delta.approximatelyEqualTo(other.delta, PrivacyBudgetLedgerConstants.DELTA_EPSILON)

private fun Float.approximatelyEqualTo(other: Float, maximumDifference: Double): Boolean {
  return abs(this - other) < maximumDifference
}

private fun Charge.toChargeWithRepetitions(repetitionCount: Int): ChargeWithRepetitions =
  ChargeWithRepetitions(epsilon, delta, repetitionCount)

private fun PrivacyBudgetBalanceEntry.toChargeWithRepetitions(): ChargeWithRepetitions =
  charge.toChargeWithRepetitions(repetitionCount)

/** Manages and updates privacy budget data. */
class PrivacyBudgetLedger(
  val backingStore: PrivacyBudgetLedgerBackingStore,
  val maximumTotalEpsilon: Float,
  val maximumTotalDelta: Float
) {

  /**
   * For each privacy bucket group in the list of PrivacyBucketGroups, checks if adding each of the
   * privacy charges to that group would make anyone of them exceed their budget.
   *
   * @throws PrivacyBudgetManagerException if there is an error commiting the transaction to the
   *   database.
   */
  suspend fun chargingWillExceedPrivacyBudget(
    privacyBucketGroups: Set<PrivacyBucketGroup>,
    charges: Set<Charge>
  ): Boolean {

    if (privacyBucketGroups.isEmpty() || charges.isEmpty()) {
      return false
    }

    backingStore.startTransaction().use { context: PrivacyBudgetLedgerTransactionContext ->
      // Check if the budget would be exceeded if charges were to be applied.=
      if (!getExceededPrivacyBuckets(context, privacyBucketGroups, charges).isEmpty()) {
        return true
      }
    }

    return false
  }

  /**
   * For each privacy bucket group in the list of PrivacyBucketGroups, adds each of the privacy
   * charges to that group.
   *
   * @throws PrivacyBudgetManagerException if the attempt to charge the privacy bucket groups was
   *   unsuccessful. Possible causes could include exceeding available privacy budget or an
   *   inability to commit an update to the database.
   */
  suspend fun charge(
    reference: Reference,
    privacyBucketGroups: Set<PrivacyBucketGroup>,
    charges: Set<Charge>
  ) {

    if (privacyBucketGroups.isEmpty() || charges.isEmpty()) {
      return
    }

    backingStore.startTransaction().use { context: PrivacyBudgetLedgerTransactionContext ->
      // First check if the budget would be exceeded if charges were to be applied.
      checkPrivacyBudgetExceeded(context, reference, privacyBucketGroups, charges)

      // Then charge the buckets
      context.addLedgerEntries(privacyBucketGroups, charges, reference)

      context.commit()
    }
  }

  suspend fun hasLedgerEntry(reference: Reference): Boolean {
    backingStore.startTransaction().use { context: PrivacyBudgetLedgerTransactionContext ->
      return context.hasLedgerEntry(reference)
    }
  }

  private suspend fun getExceededPrivacyBuckets(
    context: PrivacyBudgetLedgerTransactionContext,
    privacyBucketGroups: Set<PrivacyBucketGroup>,
    charges: Set<Charge>
  ): List<PrivacyBucketGroup> =
    privacyBucketGroups.filter {
      privacyBudgetIsExceeded(context.findIntersectingBalanceEntries(it).toSet(), charges)
    }

  private suspend fun checkPrivacyBudgetExceeded(
    context: PrivacyBudgetLedgerTransactionContext,
    privacyBucketGroups: Set<PrivacyBucketGroup>,
    charges: Set<Charge>
  ) {
    val failedBucketList = getExceededPrivacyBuckets(context, privacyBucketGroups, charges)
    if (!failedBucketList.isEmpty()) {
      throw PrivacyBudgetManagerException(PrivacyBudgetManagerExceptionType.PRIVACY_BUDGET_EXCEEDED)
    }
  }

  private suspend fun checkPrivacyBudgetExceeded(
    context: PrivacyBudgetLedgerTransactionContext,
    reference: Reference,
    privacyBucketGroups: Set<PrivacyBucketGroup>,
    charges: Set<Charge>
  ) {

    // First check if this refence key already have been proccessed.
    if (context.hasLedgerEntry(reference)) {
      return
    }

    // Then check if charging the buckets would exceed privacy budget
    if (!reference.isRefund) {
      checkPrivacyBudgetExceeded(context, privacyBucketGroups, charges)
    }
  }

  private fun exceedsUnderAdvancedComposition(
    chargeEpsilon: Float,
    chargeDelta: Float,
    numCharges: Int
  ): Boolean =
    Composition.totalPrivacyBudgetUsageUnderAdvancedComposition(
      Charge(chargeEpsilon, chargeDelta),
      numCharges,
      maximumTotalDelta
    ) > maximumTotalEpsilon

  private fun exceedsUnderSimpleComposition(charges: List<ChargeWithRepetitions>) =
    (charges.sumOf { it.totalEpsilon() } > maximumTotalEpsilon.toDouble()) ||
      (charges.sumOf { it.totalDelta() } > maximumTotalDelta.toDouble())

  /**
   * Tests whether a given privacy bucket is exceeded.
   *
   * @param balanceEntries is a list of [PrivacyBudgetBalanceEntry] that all refer to the same
   *   underlying privacy bucket.
   * @param charges is a list of charges that are to be added to this privacy bucket.
   * @return true if adding the charges would cause the total privacy budget usage for this bucket
   *   to be exceeded.
   */
  private suspend fun privacyBudgetIsExceeded(
    balanceEntries: Set<PrivacyBudgetBalanceEntry>,
    charges: Set<Charge>
  ): Boolean {

    val allCharges: List<ChargeWithRepetitions> =
      charges.map { it.toChargeWithRepetitions(1) } +
        balanceEntries.map { it.toChargeWithRepetitions() }

    // We can save some privacy budget by using the advanced composition theorem if
    // all the charges are equivalent.
    return if (allCharges.all { allCharges[0].isEquivalentTo(it) })
      exceedsUnderAdvancedComposition(
        allCharges[0].epsilon,
        allCharges[0].delta,
        allCharges.sumOf { it.count }
      )
    else exceedsUnderSimpleComposition(allCharges)
  }
}
