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

import org.wfanet.measurement.eventdataprovider.noiser.DpParams

/** Manages and updates privacy budget data. */
class PrivacyBudgetLedger(
  private val backingStore: PrivacyBudgetLedgerBackingStore,
  private val maximumTotalDpParams: DpParams,
) {
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
   * AcdpCharges to that group would make anyone of them exceed their budget. It only supports
   * Gaussian Differential Privacy noise.
   *
   * @throws PrivacyBudgetManagerException if there is an error committing the transaction to the
   *   database.
   */
  suspend fun chargingWillExceedPrivacyBudgetInAcdp(
    privacyBucketGroups: Set<PrivacyBucketGroup>,
    acdpCharges: Set<AcdpCharge>,
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

  private suspend fun getExceededPrivacyBucketsInAcdp(
    context: PrivacyBudgetLedgerTransactionContext,
    privacyBucketGroups: Set<PrivacyBucketGroup>,
    acdpCharges: Set<AcdpCharge>,
  ): List<PrivacyBucketGroup> {
    val acdpBalanceEntries = context.findAcdpBalanceEntries(privacyBucketGroups)
    val exceededEntries =
      acdpBalanceEntries.filter { exceedsUnderAcdpComposition(listOf(it.acdpCharge) + acdpCharges) }
    return exceededEntries.map { it.privacyBucketGroup }
  }

  private fun exceedsUnderAcdpComposition(acdpCharges: List<AcdpCharge>) =
    (Composition.totalPrivacyBudgetUsageUnderAcdpComposition(
      acdpCharges,
      maximumTotalDpParams.epsilon,
    ) > maximumTotalDpParams.delta)
}
