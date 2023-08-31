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

  /** Resource ID for EventGroup that fails Requisitions with CONSENT_SIGNAL_INVALID if used. */
  const val CONSENT_SIGNAL_INVALID_EVENT_GROUP_ID = "consent-signal-invalid"
  /** Resource ID for EventGroup that fails Requisitions with SPEC_INVALID if used. */
  const val SPEC_INVALID_EVENT_GROUP_ID = "spec-invalid"
  /**
   *  Resource ID for EventGroup that fails Requisitions with INSUFFICIENT_PRIVACY_BUDGET if used.
   **/
  const val INSUFFICIENT_PRIVACY_BUDGET_EVENT_GROUP_ID = "insufficient-privacy-budget"
  /** Resource ID for EventGroup that fails Requisitions with UNFULFILLABLE if used. */
  const val UNFULFILLABLE_EVENT_GROUP_ID = "unfulfillable"
  /** Resource ID for EventGroup that fails Requisitions with DECLINED if used. */
  const val DECLINED_EVENT_GROUP_ID = "declined"
}
