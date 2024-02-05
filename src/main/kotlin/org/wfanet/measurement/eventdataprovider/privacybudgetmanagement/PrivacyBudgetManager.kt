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

/**
 * This is the default value for the total amount that can be charged to a single privacy bucket.
 */
private const val MAXIMUM_PRIVACY_USAGE_PER_BUCKET = 1.0f
private const val MAXIMUM_DELTA_PER_BUCKET = 1.0e-9f

/**
 * Instantiates a privacy budget manager.
 *
 * @param filter: An object that maps [PrivacyBucketGroup]s to Event messages.
 * @param backingStore: An object that provides persistent storage of privacy budget data in a
 *   consistent and atomic manner.
 * @param maximumPrivacyBudget: The maximum privacy budget that can be used in any privacy bucket.
 * @param maximumTotalDelta: Maximum total value of the delta parameter that can be used in any
 *   privacy bucket.
 */
class PrivacyBudgetManager(
  val filter: PrivacyBucketFilter,
  private val backingStore: PrivacyBudgetLedgerBackingStore,
  private val maximumPrivacyBudget: Float = MAXIMUM_PRIVACY_USAGE_PER_BUCKET,
  private val maximumTotalDelta: Float = MAXIMUM_DELTA_PER_BUCKET,
) {

  val ledger =
    PrivacyBudgetLedger(
      backingStore,
      DpParams(maximumPrivacyBudget.toDouble(), maximumTotalDelta.toDouble()),
    )

  /** Checks if calling charge with this [reference] will result in an update in the ledger. */
  suspend fun referenceWillBeProcessed(reference: Reference) = !ledger.hasLedgerEntry(reference)

  /**
   * Checks if charging all the PrivacyBucketGroups identified by the given measurementSpec and
   * requisitionSpec in ACDP composition would not exceed ACDP privacy budget.
   *
   * @param acdpQuery represents the [AcdpQuery] that specifies charges and buckets to be charged.
   * @throws PrivacyBudgetManagerException if an error occurs in handling this request. Possible
   *   exceptions could include running out of privacy budget or a failure to commit the transaction
   *   to the database.
   */
  suspend fun chargingWillExceedPrivacyBudgetInAcdp(acdpQuery: AcdpQuery) =
    ledger.chargingWillExceedPrivacyBudgetInAcdp(
      filter.getPrivacyBucketGroups(
        acdpQuery.reference.measurementConsumerId,
        acdpQuery.landscapeMask,
      ),
      setOf(acdpQuery.acdpCharge),
    )

  /**
   * Checks if charging all the PrivacyBucketGroups identified by the given measurementSpec and
   * requisitionSpec would not exceed ACDP privacy budget, and charge the acdpCharge.
   *
   * @param acdpQuery A data class contains Reference: representing the reference
   *   keys(measurementConsumerId and referenceId which is usually requisitionId) and if the charge
   *   is a refund. LandscapeMask: eventGroupSpecs and vidSampleStart and vidSampleWidth.
   *   AcdpCharge: the AcdpCharge with rho and theta which are converted from epsilon and delta.
   * @throws PrivacyBudgetManagerException if an error occurs in handling this request. Possible
   *   exceptions could include a failure to commit the transaction to the database.
   */
  suspend fun chargePrivacyBudgetInAcdp(acdpQuery: AcdpQuery) =
    ledger.chargeInAcdp(
      acdpQuery.reference,
      filter.getPrivacyBucketGroups(
        acdpQuery.reference.measurementConsumerId,
        acdpQuery.landscapeMask,
      ),
      setOf(acdpQuery.acdpCharge),
    )
}
