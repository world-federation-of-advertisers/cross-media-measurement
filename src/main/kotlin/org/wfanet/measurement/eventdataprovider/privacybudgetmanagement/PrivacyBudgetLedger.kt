// Copyright 2022 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.eventdataprovider.privacybudgetmanagement

/** Manages and updates privacy budget data. */
internal class PrivacyBudgetLedger(val backingStore: PrivacyBudgetLedgerBackingStore) {
  /**
   * For each privacy bucket group in the list of PrivacyBucketGroups, adds each of the privacy
   * charges to that group. If successful, returns null. If an error occurs, such as exceeding
   * budget in one of the groups, then returns a PrivacyBudgetManagerReturnStatus object describing
   * the error.
   */
  fun chargePrivacyBucketGroups(
    privacyBucketGroups: List<PrivacyBucketGroup>,
    privacyCharges: List<PrivacyCharge>
  ): PrivacyBudgetManagerReturnStatus {
    throw RuntimeException("not implemented ${privacyBucketGroups} ${privacyCharges}")
  }
}
