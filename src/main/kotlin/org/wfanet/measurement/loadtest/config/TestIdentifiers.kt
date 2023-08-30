/*
 * Copyright 2022 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.loadtest.config

object TestIdentifiers {
  /** Resource ID prefix for test EventGroups created by EDP simulators. */
  const val SIMULATOR_EVENT_GROUP_REFERENCE_ID_PREFIX = "sim-eg"

  /** Resource IDs for EventGroups that fail Requisitions if used.
   */
  const val CONSENT_SIGNAL_INVALID_EVENT_GROUP_ID = "consent-signal-invalid"
  const val SPEC_INVALID_EVENT_GROUP_ID = "spec-invalid"
  const val INSUFFICIENT_PRIVACY_BUDGET_EVENT_GROUP_ID = "insufficient-privacy-budget"
  const val UNFULFILLABLE_EVENT_GROUP_ID = "unfulfillable"
  const val DECLINED_EVENT_GROUP_ID = "declined"
}
