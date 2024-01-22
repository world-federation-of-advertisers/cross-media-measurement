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

enum class PrivacyBudgetManagerExceptionType(val errorMessage: String) {
  INVALID_PRIVACY_BUCKET_FILTER("Provided Event Filter is invalid for Privacy Bucket mapping"),
  PRIVACY_BUDGET_EXCEEDED("The available privacy budget was exceeded"),
  DATABASE_UPDATE_ERROR("An error occurred committing the update to the database"),
  UPDATE_AFTER_COMMIT("Cannot update a transaction context after a commit"),
  NESTED_TRANSACTION("Backing Store doesn't support nested transactions"),
  BACKING_STORE_CLOSED("Cannot start a transaction after closing the backing store"),
  INCORRECT_NOISE_MECHANISM(
    "Noise mechanism should be DISCRETE_GAUSSIAN or GAUSSIAN for ACDP composition"
  ),
}

/** An exception thrown by the privacy budget manager. */
class PrivacyBudgetManagerException(
  // TODO(@uakyol @duliomatos) refactor this exception to use a sealed exception class hierarchy
  val errorType: PrivacyBudgetManagerExceptionType,
  cause: Throwable? = null,
) : Exception(errorType.errorMessage, cause)
