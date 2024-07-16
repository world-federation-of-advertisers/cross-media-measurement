// Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.mill.liquidlegionsv2.crypto

import org.wfanet.anysketch.crypto.CombineElGamalPublicKeysRequest
import org.wfanet.anysketch.crypto.CombineElGamalPublicKeysResponse
import org.wfanet.measurement.internal.duchy.protocol.CompleteReachOnlyExecutionPhaseAtAggregatorRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteReachOnlyExecutionPhaseAtAggregatorResponse
import org.wfanet.measurement.internal.duchy.protocol.CompleteReachOnlyExecutionPhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteReachOnlyExecutionPhaseResponse
import org.wfanet.measurement.internal.duchy.protocol.CompleteReachOnlyInitializationPhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteReachOnlyInitializationPhaseResponse
import org.wfanet.measurement.internal.duchy.protocol.CompleteReachOnlySetupPhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteReachOnlySetupPhaseResponse

/**
 * Crypto operations for the Reach Only Liquid Legions v2 protocol. check
 * src/main/cc/wfa/measurement/common/crypto/reach_only_liquid_legions_v2_encryption_utility.h for
 * more descriptions.
 */
interface ReachOnlyLiquidLegionsV2Encryption {

  fun completeReachOnlyInitializationPhase(
    request: CompleteReachOnlyInitializationPhaseRequest
  ): CompleteReachOnlyInitializationPhaseResponse

  fun completeReachOnlySetupPhase(
    request: CompleteReachOnlySetupPhaseRequest
  ): CompleteReachOnlySetupPhaseResponse

  fun completeReachOnlySetupPhaseAtAggregator(
    request: CompleteReachOnlySetupPhaseRequest
  ): CompleteReachOnlySetupPhaseResponse

  fun completeReachOnlyExecutionPhase(
    request: CompleteReachOnlyExecutionPhaseRequest
  ): CompleteReachOnlyExecutionPhaseResponse

  fun completeReachOnlyExecutionPhaseAtAggregator(
    request: CompleteReachOnlyExecutionPhaseAtAggregatorRequest
  ): CompleteReachOnlyExecutionPhaseAtAggregatorResponse

  fun combineElGamalPublicKeys(
    request: CombineElGamalPublicKeysRequest
  ): CombineElGamalPublicKeysResponse
}
