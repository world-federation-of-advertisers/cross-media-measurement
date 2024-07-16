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
import org.wfanet.measurement.internal.duchy.protocol.shareshuffle.HonestMajorityShareShuffleUtility

/**
 * A [HonestMajorityShareShuffleCryptor] implementation using the JNI
 * [HonestMajorityShareShuffleUtility].
 */
class JniHonestMajorityShareShuffleCryptor : HonestMajorityShareShuffleCryptor {

  override fun completeReachAndFrequencyShufflePhase(
    request: CompleteShufflePhaseRequest
  ): CompleteShufflePhaseResponse {
    return CompleteShufflePhaseResponse.parseFrom(
      HonestMajorityShareShuffleUtility.completeReachAndFrequencyShufflePhase(request.toByteArray())
    )
  }

  override fun completeReachAndFrequencyAggregationPhase(
    request: CompleteAggregationPhaseRequest
  ): CompleteAggregationPhaseResponse {
    return CompleteAggregationPhaseResponse.parseFrom(
      HonestMajorityShareShuffleUtility.completeReachAndFrequencyAggregationPhase(
        request.toByteArray()
      )
    )
  }

  override fun completeReachOnlyShufflePhase(
    request: CompleteShufflePhaseRequest
  ): CompleteShufflePhaseResponse {
    return CompleteShufflePhaseResponse.parseFrom(
      HonestMajorityShareShuffleUtility.completeReachOnlyShufflePhase(request.toByteArray())
    )
  }

  override fun completeReachOnlyAggregationPhase(
    request: CompleteAggregationPhaseRequest
  ): CompleteAggregationPhaseResponse {
    return CompleteAggregationPhaseResponse.parseFrom(
      HonestMajorityShareShuffleUtility.completeReachOnlyAggregationPhase(request.toByteArray())
    )
  }

  companion object {
    init {
      System.loadLibrary("honest_majority_share_shuffle_utility")
    }
  }
}
