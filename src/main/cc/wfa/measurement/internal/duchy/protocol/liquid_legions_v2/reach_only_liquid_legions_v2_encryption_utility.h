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

#ifndef SRC_MAIN_CC_WFA_MEASUREMENT_INTERNAL_DUCHY_PROTOCOL_REACH_ONLY_LIQUID_LEGIONS_V2_LIQUID_LEGIONS_V2_ENCRYPTION_UTILITY_H_
#define SRC_MAIN_CC_WFA_MEASUREMENT_INTERNAL_DUCHY_PROTOCOL_REACH_ONLY_LIQUID_LEGIONS_V2_LIQUID_LEGIONS_V2_ENCRYPTION_UTILITY_H_

#include "absl/status/statusor.h"
#include "wfa/measurement/internal/duchy/protocol/reach_only_liquid_legions_v2_encryption_methods.pb.h"

namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2 {

using ::wfa::measurement::internal::duchy::protocol::
    CompleteReachOnlyExecutionPhaseAtAggregatorRequest;
using ::wfa::measurement::internal::duchy::protocol::
    CompleteReachOnlyExecutionPhaseAtAggregatorResponse;
using ::wfa::measurement::internal::duchy::protocol::
    CompleteReachOnlyExecutionPhaseRequest;
using ::wfa::measurement::internal::duchy::protocol::
    CompleteReachOnlyExecutionPhaseResponse;
using ::wfa::measurement::internal::duchy::protocol::
    CompleteReachOnlyInitializationPhaseRequest;
using ::wfa::measurement::internal::duchy::protocol::
    CompleteReachOnlyInitializationPhaseResponse;
using ::wfa::measurement::internal::duchy::protocol::
    CompleteReachOnlySetupPhaseAtAggregatorResponse;
using ::wfa::measurement::internal::duchy::protocol::
    CompleteReachOnlySetupPhaseRequest;
using ::wfa::measurement::internal::duchy::protocol::
    CompleteReachOnlySetupPhaseResponse;

// Complete work in the initialization phase at both the aggregator and
// non-aggregator workers. More specifically, the worker would generate a random
// set of ElGamal Key pair.
absl::StatusOr<CompleteReachOnlyInitializationPhaseResponse>
CompleteReachOnlyInitializationPhase(
    const CompleteReachOnlyInitializationPhaseRequest& request);

// Complete work in the setup phase at the non-aggregator workers. More
// specifically, the worker would
//   1. add local noise registers (if configured to).
//   2. shuffle all registers.
//   3. stores the amount of excessive noise that it can remove to the database.
absl::StatusOr<CompleteReachOnlySetupPhaseResponse> CompleteReachOnlySetupPhase(
    const CompleteReachOnlySetupPhaseRequest& request);

// Complete work in the setup phase at the aggregator. More specifically, the
// aggregator would
//   1. add local noise registers (if configured to).
//   2. shuffle all registers.
//   3. sample a Paillier keypair
//   4. encrypt the excessive noise using Paillier encryption.
absl::StatusOr<CompleteReachOnlySetupPhaseResponse>
CompleteReachOnlySetupPhaseAtAggregator(
    const CompleteReachOnlySetupPhaseRequest& request);

//  Complete work in the execution phase one at a non-aggregator worker.
//  More specifically, the worker would
//    1. blind the positions (decrypt local ElGamal layer and then add another
//       layer of deterministic pohlig_hellman encryption.
//    2. re-randomize keys and counts.
//    3. shuffle all registers.
//    4. adds its excessive noise to the ciphertext that stores the aggregated
//    excessive noise to be removed.
absl::StatusOr<CompleteReachOnlyExecutionPhaseResponse>
CompleteReachOnlyExecutionPhase(
    const CompleteReachOnlyExecutionPhaseRequest& request);

//  Complete work in the execution phase one at the aggregator worker.
//  More specifically, the worker would
//    1. decrypt the local ElGamal encryption on the positions.
//    2. join the registers by positions.
//    3. count the number of unique registers, excluding the blinded histogram
//    noise and the publisher noise.
//    4. decrypt the Paillier ciphertext that stores the aggregated excessive
//    noise and subtract it from the total register count.
absl::StatusOr<CompleteReachOnlyExecutionPhaseAtAggregatorResponse>
CompleteReachOnlyExecutionPhaseAtAggregator(
    const CompleteReachOnlyExecutionPhaseAtAggregatorRequest& request);

}  // namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2

#endif  // SRC_MAIN_CC_WFA_MEASUREMENT_INTERNAL_DUCHY_PROTOCOL_REACH_ONLY_LIQUID_LEGIONS_V2_LIQUID_LEGIONS_V2_ENCRYPTION_UTILITY_H_
