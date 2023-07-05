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

data class DpChargeWithRepetitions(val epsilon: Float, val delta: Float, val count: Int)

private fun DpChargeWithRepetitions.totalEpsilon(): Double = epsilon.toDouble() * count

private fun DpChargeWithRepetitions.totalDelta(): Double = delta.toDouble() * count

private fun DpChargeWithRepetitions.isEquivalentTo(other: DpChargeWithRepetitions): Boolean =
  this.epsilon.approximatelyEqualTo(other.epsilon, PrivacyBudgetLedgerConstants.EPSILON_EPSILON) &&
    this.delta.approximatelyEqualTo(other.delta, PrivacyBudgetLedgerConstants.DELTA_EPSILON)

private fun Float.approximatelyEqualTo(other: Float, maximumDifference: Double): Boolean {
  return abs(this - other) < maximumDifference
}

private fun DpCharge.toChargeWithRepetitions(repetitionCount: Int): DpChargeWithRepetitions =
  DpChargeWithRepetitions(epsilon, delta, repetitionCount)

private fun PrivacyBudgetBalanceEntry.toChargeWithRepetitions(): DpChargeWithRepetitions =
  dpCharge.toChargeWithRepetitions(repetitionCount)

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
    dpCharges: Set<DpCharge>
  ): Boolean {

    if (privacyBucketGroups.isEmpty() || dpCharges.isEmpty()) {
      return false
    }

    backingStore.startTransaction().use { context: PrivacyBudgetLedgerTransactionContext ->
      // Check if the budget would be exceeded if charges were to be applied.=
      if (!getExceededPrivacyBuckets(context, privacyBucketGroups, dpCharges).isEmpty()) {
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
    dpCharges: Set<DpCharge>
  ) {

    if (privacyBucketGroups.isEmpty() || dpCharges.isEmpty()) {
      return
    }

    backingStore.startTransaction().use { context: PrivacyBudgetLedgerTransactionContext ->
      // First check if the budget would be exceeded if charges were to be applied.
      checkPrivacyBudgetExceeded(context, reference, privacyBucketGroups, dpCharges)

      // Then charge the buckets
      context.addLedgerEntries(privacyBucketGroups, dpCharges, reference)

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
    dpCharges: Set<DpCharge>
  ): List<PrivacyBucketGroup> =
    privacyBucketGroups.filter {
      privacyBudgetIsExceeded(context.findIntersectingBalanceEntries(it).toSet(), dpCharges)
    }

  private suspend fun checkPrivacyBudgetExceeded(
    context: PrivacyBudgetLedgerTransactionContext,
    privacyBucketGroups: Set<PrivacyBucketGroup>,
    dpCharges: Set<DpCharge>
  ) {
    val failedBucketList = getExceededPrivacyBuckets(context, privacyBucketGroups, dpCharges)
    if (!failedBucketList.isEmpty()) {
      throw PrivacyBudgetManagerException(PrivacyBudgetManagerExceptionType.PRIVACY_BUDGET_EXCEEDED)
    }
  }

  private suspend fun checkPrivacyBudgetExceeded(
    context: PrivacyBudgetLedgerTransactionContext,
    reference: Reference,
    privacyBucketGroups: Set<PrivacyBucketGroup>,
    dpCharges: Set<DpCharge>
  ) {

    // First check if this refence key already have been proccessed.
    if (context.hasLedgerEntry(reference)) {
      return
    }

    // Then check if charging the buckets would exceed privacy budget
    if (!reference.isRefund) {
      checkPrivacyBudgetExceeded(context, privacyBucketGroups, dpCharges)
    }
  }

  private fun exceedsUnderAdvancedComposition(
    chargeEpsilon: Float,
    chargeDelta: Float,
    numCharges: Int
  ): Boolean =
    Composition.totalPrivacyBudgetUsageUnderAdvancedComposition(
      DpCharge(chargeEpsilon, chargeDelta),
      numCharges,
      maximumTotalDelta
    ) > maximumTotalEpsilon

  private fun exceedsUnderSimpleComposition(dpCharges: List<DpChargeWithRepetitions>) =
    (dpCharges.sumOf { it.totalEpsilon() } > maximumTotalEpsilon.toDouble()) ||
      (dpCharges.sumOf { it.totalDelta() } > maximumTotalDelta.toDouble())

  private fun exceedsUnderAcdpComposition(acdpCharges: List<AcdpCharge>) =
    (Composition.totalPrivacyBudgetUsageUnderAcdpComposition(acdpCharges, maximumTotalEpsilon) >
      maximumTotalDelta)

  /**
   * Tests whether a given privacy bucket is exceeded.
   *
   * @param balanceEntries is a list of [PrivacyBudgetBalanceEntry] that all refer to the same
   *   underlying privacy bucket.
   * @param dpCharges is a list of dpCharges that are to be added to this privacy bucket.
   * @return true if adding the charges would cause the total privacy budget usage for this bucket
   *   to be exceeded.
   */
  private fun privacyBudgetIsExceeded(
    balanceEntries: Set<PrivacyBudgetBalanceEntry>,
    dpCharges: Set<DpCharge>
  ): Boolean {

    val allDpCharges: List<DpChargeWithRepetitions> =
      dpCharges.map { it.toChargeWithRepetitions(1) } +
        balanceEntries.map { it.toChargeWithRepetitions() }

    // We can save some privacy budget by using the advanced composition theorem if
    // all the dpCharges are equivalent.
    return if (allDpCharges.all { allDpCharges[0].isEquivalentTo(it) })
      exceedsUnderAdvancedComposition(
        allDpCharges[0].epsilon,
        allDpCharges[0].delta,
        allDpCharges.sumOf { it.count }
      )
    else exceedsUnderSimpleComposition(allDpCharges)
  }
}
