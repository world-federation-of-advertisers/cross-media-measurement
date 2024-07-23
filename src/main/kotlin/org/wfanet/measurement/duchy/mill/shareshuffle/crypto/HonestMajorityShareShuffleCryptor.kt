// Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.mill.shareshuffle.crypto

import org.wfanet.measurement.internal.duchy.protocol.CompleteAggregationPhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteAggregationPhaseResponse
import org.wfanet.measurement.internal.duchy.protocol.CompleteShufflePhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteShufflePhaseResponse

/**
 * Crypto operations for the Honest Majority Share Shuffle protocol. check
 * src/main/cc/wfa/measurement/internal/duchy/protocol/share_shuffle/
 * honest_majority_share_shuffle_utility.h for more descriptions.
 */
interface HonestMajorityShareShuffleCryptor {
  fun completeReachAndFrequencyShufflePhase(
    request: CompleteShufflePhaseRequest
  ): CompleteShufflePhaseResponse

  fun completeReachAndFrequencyAggregationPhase(
    request: CompleteAggregationPhaseRequest
  ): CompleteAggregationPhaseResponse

  fun completeReachOnlyShufflePhase(
    request: CompleteShufflePhaseRequest
  ): CompleteShufflePhaseResponse

  fun completeReachOnlyAggregationPhase(
    request: CompleteAggregationPhaseRequest
  ): CompleteAggregationPhaseResponse
}
