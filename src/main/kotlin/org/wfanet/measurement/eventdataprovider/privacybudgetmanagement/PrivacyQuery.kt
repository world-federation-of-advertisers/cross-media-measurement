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

import org.wfanet.measurement.common.OpenEndTimeRange

/** Represents a charge that will be made to a privacy budget */
data class Charge(val epsilon: Float, val delta: Float)

/**
 * Represents an element that caused charges to the manager and wheter or not if those charges were
 * positive or refunds. [referenceKey] is usally requisitionId. [referenceId] and [isRefund] can be
 * null for when calling chargingWillExceedPrivacyBudget.
 */
data class Reference(
  val measurementConsumerId: String,
  val referenceId: String,
  val isRefund: Boolean
)

/** Represents a privacy filter for one event group. */
data class EventGroupSpec(val eventFilter: String, val timeRange: OpenEndTimeRange)

/** Represents a mask to the PrivacyLandscape. */
data class LandscapeMask(
  val eventGroupSpecs: List<EventGroupSpec>,
  val vidSampleStart: Float,
  val vidSampleWidth: Float
)

/** Represents multiple charges to the multiple buckets in the PrivacyLandscape. */
data class Query(val reference: Reference, val landscapeMask: LandscapeMask, val charge: Charge)
