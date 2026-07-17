/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.computation

/**
 * Represents small-cell suppression constraints for a dataset.
 *
 * @property minUsers The minimum number of unique users required for the data to be considered
 *   above minimum thresholds.
 * @property minImpressions The minimum number of impressions required to satisfy small-cell
 *   suppression.
 * @property reachMaxFrequencyPerUser The max frequency per user for reach use cases. Required if
 *   differential privacy is applied in addition to small-cell suppression. It must be greater than
 *   zero and less than HMShuffle Ring Modulus and less than or equal to Byte.MAX_VALUE.
 */
data class ResultMinimumThresholds(
  val minUsers: Int,
  val minImpressions: Int,
  val reachMaxFrequencyPerUser: Int =
    minOf(Byte.MAX_VALUE.toInt(), ComputationParams.MIN_RING_MODULUS - 1),
)
