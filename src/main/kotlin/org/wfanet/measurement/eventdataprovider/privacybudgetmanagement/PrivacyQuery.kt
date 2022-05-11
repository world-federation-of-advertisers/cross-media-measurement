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

import java.time.LocalDate

/** Represents a charge that will be made to a privacy budget */
data class PrivacyCharge(val epsilon: Float, val delta: Float)

/**
 * Represents an element that caused charges to the manager and wheter or not if those charges were
 * positive or refunds. [referenceKey] is usally requisitionId
 */
data class PrivacyReference(val referenceKey: String, val isPositive: Boolean)

/** Represents a privacy filter for one event group. */
data class PrivacyEventGroupSpec(
  val eventFilter: String,
  val startDate: LocalDate,
  val endDate: LocalDate
)

/** Represents multiple charges to the multiple buckets in the PrivacyLandscape. */
data class PrivacyQuery(
  val privacyReference: PrivacyReference,
  val privacyEventGroupSpecs: List<PrivacyEventGroupSpec>,
  val vidSampleStart: Float,
  val vidSampleWidth: Float,
  val privacyCharge: PrivacyCharge,
  val operativePrivacyFields: Set<String>
)
