/*
 * Copyright 2025 The Cross-Media Measurement Authors
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
package org.wfanet.measurement.privacybudgetmanager

enum class LedgerExceptionType(val errorMessage: String) {
  INVALID_PRIVACY_LANDSCAPE_IDS("Some queries are not targetting the active landscape"),
  TABLE_NOT_READY(
    "Database is not ready to receive queries for Given privacy landscape identifier."
  ),
  TABLE_METADATA_DOESNT_EXIST(
    "Given privacy landscape identifier not found in PrivacyChargesMetadata table."
  ),
}

/** An exception thrown by the privacy budget manager ledger. */
class LedgerException(
  val errorType: LedgerExceptionType,
  val details: String? = null, // Make details a nullable String
  cause: Throwable? = null,
) :
  Exception(
    "${errorType.errorMessage}${if (!details.isNullOrBlank()) ": $details" else ""}".trim(),
    cause,
  )
