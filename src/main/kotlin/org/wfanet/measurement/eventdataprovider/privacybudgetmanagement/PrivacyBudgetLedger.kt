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
import org.wfanet.measurement.eventdataprovider.noiser.DpParams

/** Manages and updates privacy budget data. */
class PrivacyBudgetLedger(
  private val backingStore: PrivacyBudgetLedgerBackingStore,
  private val maximumTotalDpParams: DpParams,
) {
  /**
   * For each PrivacyBucketGroup in the list of PrivacyBucketGroups, adds each of the DpCharges to
   * that group and check privacy budget usage using Advanced composition.
   *
   * This charge method only supports Laplace Differential Privacy noise and should be deprecated
   * once the system is completely migrated to Gaussian Differential Privacy noise and ACDP
   * composition.
   *
   * @throws PrivacyBudgetManagerException if the attempt to charge the privacy bucket groups was
   *   unsuccessful. Possible causes could include exceeding available privacy budget or an
   *   inability to commit an update to the database.
   */
  @Deprecated(
    "Should be removed after completely switching to Gaussian noise and ACDP composition",
  )
  suspend fun charge(
    reference: Reference,
    privacyBucketGroups: Set<PrivacyBucketGroup>,
    dpCharges: Set<DpCharge>,
  ) {
    if (privacyBucketGroups.isEmpty() || dpCharges.isEmpty()) {
      return
    }

    backingStore.startTransaction().use { context: PrivacyBudgetLedgerTransactionContext ->
      // First check if the budget would be exceeded if dpCharges were to be applied.
      checkPrivacyBudgetExceeded(context, reference, privacyBucketGroups, dpCharges)

      // Then charge the buckets
      context.addLedgerEntries(privacyBucketGroups, dpCharges, reference)

      context.commit()
    }
  }

  /**
   * For each PrivacyBucketGroup in the list of PrivacyBucketGroups, adds each of the AcdpCharges to
   * that group and check privacy budget usage using ACDP composition. It only supports Gaussian
   * Differential Privacy noise.
   *
   * @throws PrivacyBudgetManagerException if the attempt to charge the PrivacyBucketGroups was
   *   unsuccessful. Possible causes could include exceeding available privacy budget or an
   *   inability to commit an update to the database.
   */
  suspend fun chargeInAcdp(
    reference: Reference,
    privacyBucketGroups: Set<PrivacyBucketGroup>,
    acdpCharges: Set<AcdpCharge>,
  ) {
    if (privacyBucketGroups.isEmpty() || acdpCharges.isEmpty()) {
      return
    }

    backingStore.startTransaction().use { context: PrivacyBudgetLedgerTransactionContext ->
      // First check if the budget would be exceeded if acdpCharges were to be applied.
      checkPrivacyBudgetExceededInAcdp(context, reference, privacyBucketGroups, acdpCharges)

      // Then charge the buckets
      context.addAcdpLedgerEntries(privacyBucketGroups, acdpCharges, reference)

      context.commit()
    }
  }

  suspend fun hasLedgerEntry(reference: Reference): Boolean {
    backingStore.startTransaction().use { context: PrivacyBudgetLedgerTransactionContext ->
      return context.hasLedgerEntry(reference)
    }
  }

  /**
   * For each PrivacyBucketGroup in the list of PrivacyBucketGroups, checks if adding each of the
   * DpCharges to that group would make anyone of them exceed their budget.
   *
   * This method only supports Laplace Differential Privacy noise and should be deprecated once the
   * system is completely migrated to Gaussian Differential Privacy noise and ACDP composition.
   *
   * @throws PrivacyBudgetManagerException if there is an error committing the transaction to the
   *   database.
   */
  @Deprecated(
    "Should be removed after completely switching to Gaussian noise and ACDP composition",
  )
  suspend fun chargingWillExceedPrivacyBudget(
    privacyBucketGroups: Set<PrivacyBucketGroup>,
    dpCharges: Set<DpCharge>
  ): Boolean {
    if (privacyBucketGroups.isEmpty() || dpCharges.isEmpty()) {
      return false
    }

    backingStore.startTransaction().use { context: PrivacyBudgetLedgerTransactionContext ->
      // Check if the budget would be exceeded if dpCharges were to be applied.
      if (getExceededPrivacyBuckets(context, privacyBucketGroups, dpCharges).isNotEmpty()) {
        return true
      }
    }

    return false
  }

  /**
   * For each PrivacyBucketGroup in the list of PrivacyBucketGroups, checks if adding each of the
   * AcdpCharges to that group would make anyone of them exceed their budget. It only supports
   * Gaussian Differential Privacy noise.
   *
   * @throws PrivacyBudgetManagerException if there is an error committing the transaction to the
   *   database.
   */
  suspend fun chargingWillExceedPrivacyBudgetInAcdp(
    privacyBucketGroups: Set<PrivacyBucketGroup>,
    acdpCharges: Set<AcdpCharge>
  ): Boolean {
    if (privacyBucketGroups.isEmpty() || acdpCharges.isEmpty()) {
      return false
    }

    backingStore.startTransaction().use { context: PrivacyBudgetLedgerTransactionContext ->
      // Check if the budget would be exceeded if acdpCharges were to be applied.
      if (getExceededPrivacyBucketsInAcdp(context, privacyBucketGroups, acdpCharges).isNotEmpty()) {
        return true
      }
    }

    return false
  }

  private suspend fun checkPrivacyBudgetExceeded(
    context: PrivacyBudgetLedgerTransactionContext,
    reference: Reference,
    privacyBucketGroups: Set<PrivacyBucketGroup>,
    dpCharges: Set<DpCharge>,
  ) {
    // First check if this reference key already have been processed.
    if (context.hasLedgerEntry(reference)) {
      return
    }

    // Then check if charging the buckets would exceed privacy budget
    if (!reference.isRefund) {
      checkPrivacyBudgetExceeded(context, privacyBucketGroups, dpCharges)
    }
  }

  private suspend fun checkPrivacyBudgetExceededInAcdp(
    context: PrivacyBudgetLedgerTransactionContext,
    reference: Reference,
    privacyBucketGroups: Set<PrivacyBucketGroup>,
    acdpCharges: Set<AcdpCharge>,
  ) {
    // First check if this reference key already have been processed.
    if (context.hasLedgerEntry(reference)) {
      return
    }

    // Then check if charging the buckets would exceed privacy budget
    if (!reference.isRefund) {
      checkPrivacyBudgetExceededInAcdp(context, privacyBucketGroups, acdpCharges)
    }
  }

  private suspend fun checkPrivacyBudgetExceeded(
    context: PrivacyBudgetLedgerTransactionContext,
    privacyBucketGroups: Set<PrivacyBucketGroup>,
    dpCharges: Set<DpCharge>,
  ) {
    val failedBucketList = getExceededPrivacyBuckets(context, privacyBucketGroups, dpCharges)
    if (failedBucketList.isNotEmpty()) {
      throw PrivacyBudgetManagerException(PrivacyBudgetManagerExceptionType.PRIVACY_BUDGET_EXCEEDED)
    }
  }

  private suspend fun checkPrivacyBudgetExceededInAcdp(
    context: PrivacyBudgetLedgerTransactionContext,
    privacyBucketGroups: Set<PrivacyBucketGroup>,
    acdpCharges: Set<AcdpCharge>,
  ) {
    val failedBucketList =
      getExceededPrivacyBucketsInAcdp(context, privacyBucketGroups, acdpCharges)
    if (failedBucketList.isNotEmpty()) {
      throw PrivacyBudgetManagerException(PrivacyBudgetManagerExceptionType.PRIVACY_BUDGET_EXCEEDED)
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

  private suspend fun getExceededPrivacyBucketsInAcdp(
    context: PrivacyBudgetLedgerTransactionContext,
    privacyBucketGroups: Set<PrivacyBucketGroup>,
    acdpCharges: Set<AcdpCharge>
  ): List<PrivacyBucketGroup> =
    privacyBucketGroups.filter { privacyBucketGroup ->
      val acdpBalanceEntries = setOf(context.findAcdpBalanceEntry(privacyBucketGroup))
      val balanceAcdpCharges = acdpBalanceEntries.map { it.acdpCharge }
      exceedsUnderAcdpComposition(balanceAcdpCharges + acdpCharges)
    }

  private fun exceedsUnderAdvancedComposition(
    chargeEpsilon: Float,
    chargeDelta: Float,
    numCharges: Int
  ): Boolean =
    Composition.totalPrivacyBudgetUsageUnderAdvancedComposition(
      DpCharge(chargeEpsilon, chargeDelta),
      numCharges,
      maximumTotalDpParams.delta.toFloat()
    ) > maximumTotalDpParams.epsilon.toFloat()

  private fun exceedsUnderSimpleComposition(dpCharges: List<DpChargeWithRepetitions>) =
    (dpCharges.sumOf { it.totalEpsilon() } > maximumTotalDpParams.epsilon) ||
      (dpCharges.sumOf { it.totalDelta() } > maximumTotalDpParams.delta)

  private fun exceedsUnderAcdpComposition(acdpCharges: List<AcdpCharge>) =
    (Composition.totalPrivacyBudgetUsageUnderAcdpComposition(
      acdpCharges,
      maximumTotalDpParams.epsilon
    ) > maximumTotalDpParams.delta)

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

private object PrivacyBudgetLedgerConstants {
  /**
   * Two differential privacy values are considered equal if they are within this amount of each
   * other.
   */
  const val EPSILON_EPSILON = 1.0E-9
  const val DELTA_EPSILON = 1.0E-12
}

private data class DpChargeWithRepetitions(val epsilon: Float, val delta: Float, val count: Int)

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
