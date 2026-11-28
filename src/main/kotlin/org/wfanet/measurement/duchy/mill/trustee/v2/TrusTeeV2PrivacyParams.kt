/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.duchy.mill.trustee.v2

import org.wfanet.measurement.computation.ResultMinimumThresholds
import org.wfanet.measurement.internal.duchy.NoiseMechanism

/**
 * Privacy parameters of the TrusTEE v2 Mill.
 *
 * These are compiled into the attested image so that no operator or caller can weaken them.
 */
object TrusTeeV2PrivacyParams {
  // DO_NOT_SUBMIT(world-federation-of-advertisers/cross-media-measurement#4532): Replace these
  // placeholder thresholds with the agreed production values.

  /** Minimum noised reach of a released cell and of each `DataProvider`'s slice. */
  const val MIN_USERS = 10

  /** Minimum noised impressions of a released cell. */
  const val MIN_IMPRESSIONS = 10

  /** Most impressions one VID contributes to a cell, the sensitivity of the impression draw. */
  const val REACH_MAX_FREQUENCY_PER_USER = 10

  /** [CellPrivacyParams] of every TrusTEE v2 computation. */
  val CELL_PRIVACY_PARAMS =
    CellPrivacyParams(
      NoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE,
      ResultMinimumThresholds(
        minUsers = MIN_USERS,
        minImpressions = MIN_IMPRESSIONS,
        reachMaxFrequencyPerUser = REACH_MAX_FREQUENCY_PER_USER,
      ),
    )
}
