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

enum class PrivacyBudgetManagerExceptionType(val errorMessage: String) {
  INVLAID_PRIVACY_BUCKET_FILTER("Provided Event Filter is invalid for Privacy Bucket mapping"),
  PRIVACY_BUDGET_EXCEEDED("The available privacy budget was exceeded"),
  DATABASE_UPDATE_ERROR("An error occurred committing the update to the database")
}

/** An exception thrown by the privacy budget manager. */
class PrivacyBudgetManagerException(
  val errorType: PrivacyBudgetManagerExceptionType,
  val privacyBuckets: List<PrivacyBucketGroup>
) : Exception(errorType.errorMessage)
