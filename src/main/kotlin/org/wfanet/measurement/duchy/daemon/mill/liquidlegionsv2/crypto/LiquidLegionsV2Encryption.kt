// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.daemon.mill.liquidlegionsv2.crypto

import org.wfanet.measurement.protocol.CompleteExecutionPhaseOneAtAggregatorRequest
import org.wfanet.measurement.protocol.CompleteExecutionPhaseOneAtAggregatorResponse
import org.wfanet.measurement.protocol.CompleteExecutionPhaseOneRequest
import org.wfanet.measurement.protocol.CompleteExecutionPhaseOneResponse
import org.wfanet.measurement.protocol.CompleteExecutionPhaseThreeAtAggregatorRequest
import org.wfanet.measurement.protocol.CompleteExecutionPhaseThreeAtAggregatorResponse
import org.wfanet.measurement.protocol.CompleteExecutionPhaseThreeRequest
import org.wfanet.measurement.protocol.CompleteExecutionPhaseThreeResponse
import org.wfanet.measurement.protocol.CompleteExecutionPhaseTwoAtAggregatorRequest
import org.wfanet.measurement.protocol.CompleteExecutionPhaseTwoAtAggregatorResponse
import org.wfanet.measurement.protocol.CompleteExecutionPhaseTwoRequest
import org.wfanet.measurement.protocol.CompleteExecutionPhaseTwoResponse
import org.wfanet.measurement.protocol.CompleteSetupPhaseRequest
import org.wfanet.measurement.protocol.CompleteSetupPhaseResponse

/**
 * Crypto operations for the Liquid Legions v2 protocol.
 * check src/main/cc/wfa/measurement/common/crypto/liquid_legions_v2_encryption_utility.h for more
 * descriptions.
 */
interface LiquidLegionsV2Encryption {

  fun completeSetupPhase(request: CompleteSetupPhaseRequest): CompleteSetupPhaseResponse

  fun completeExecutionPhaseOne(request: CompleteExecutionPhaseOneRequest):
    CompleteExecutionPhaseOneResponse

  fun completeExecutionPhaseOneAtAggregator(
    request: CompleteExecutionPhaseOneAtAggregatorRequest
  ):
    CompleteExecutionPhaseOneAtAggregatorResponse

  fun completeExecutionPhaseTwo(request: CompleteExecutionPhaseTwoRequest):
    CompleteExecutionPhaseTwoResponse

  fun completeExecutionPhaseTwoAtAggregator(request: CompleteExecutionPhaseTwoAtAggregatorRequest):
    CompleteExecutionPhaseTwoAtAggregatorResponse

  fun completeExecutionPhaseThree(request: CompleteExecutionPhaseThreeRequest):
    CompleteExecutionPhaseThreeResponse

  fun completeExecutionPhaseThreeAtAggregator(
    request: CompleteExecutionPhaseThreeAtAggregatorRequest
  ):
    CompleteExecutionPhaseThreeAtAggregatorResponse
}
