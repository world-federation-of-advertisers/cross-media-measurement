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

#ifndef SRC_MAIN_CC_WFA_MEASUREMENT_INTERNAL_DUCHY_PROTOCOL_LIQUID_LEGIONS_V2_LIQUID_LEGIONS_V2_ENCRYPTION_UTILITY_H_
#define SRC_MAIN_CC_WFA_MEASUREMENT_INTERNAL_DUCHY_PROTOCOL_LIQUID_LEGIONS_V2_LIQUID_LEGIONS_V2_ENCRYPTION_UTILITY_H_

#include "absl/status/statusor.h"
#include "wfa/measurement/internal/duchy/protocol/liquid_legions_v2_encryption_methods.pb.h"

namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2 {

using ::wfa::measurement::internal::duchy::protocol::
    CompleteExecutionPhaseOneAtAggregatorRequest;
using ::wfa::measurement::internal::duchy::protocol::
    CompleteExecutionPhaseOneAtAggregatorResponse;
using ::wfa::measurement::internal::duchy::protocol::
    CompleteExecutionPhaseOneRequest;
using ::wfa::measurement::internal::duchy::protocol::
    CompleteExecutionPhaseOneResponse;
using ::wfa::measurement::internal::duchy::protocol::
    CompleteExecutionPhaseThreeAtAggregatorRequest;
using ::wfa::measurement::internal::duchy::protocol::
    CompleteExecutionPhaseThreeAtAggregatorResponse;
using ::wfa::measurement::internal::duchy::protocol::
    CompleteExecutionPhaseThreeRequest;
using ::wfa::measurement::internal::duchy::protocol::
    CompleteExecutionPhaseThreeResponse;
using ::wfa::measurement::internal::duchy::protocol::
    CompleteExecutionPhaseTwoAtAggregatorRequest;
using ::wfa::measurement::internal::duchy::protocol::
    CompleteExecutionPhaseTwoAtAggregatorResponse;
using ::wfa::measurement::internal::duchy::protocol::
    CompleteExecutionPhaseTwoRequest;
using ::wfa::measurement::internal::duchy::protocol::
    CompleteExecutionPhaseTwoResponse;
using ::wfa::measurement::internal::duchy::protocol::
    CompleteInitializationPhaseRequest;
using ::wfa::measurement::internal::duchy::protocol::
    CompleteInitializationPhaseResponse;
using ::wfa::measurement::internal::duchy::protocol::CompleteSetupPhaseRequest;
using ::wfa::measurement::internal::duchy::protocol::CompleteSetupPhaseResponse;

// Complete work in the initialization phase at both the aggregator and
// non-aggregator workers. More specifically, the worker would generate a random
// set of ElGamal Key pair.
absl::StatusOr<CompleteInitializationPhaseResponse> CompleteInitializationPhase(
    const CompleteInitializationPhaseRequest& request);

// Complete work in the setup phase at both the aggregator and non-aggregator
// workers. More specifically, the worker would
//   1. add local noise registers (if configured to).
//   2. shuffle all registers.
absl::StatusOr<CompleteSetupPhaseResponse> CompleteSetupPhase(
    const CompleteSetupPhaseRequest& request);

//  Complete work in the execution phase one at a non-aggregator worker.
//  More specifically, the worker would
//    1. blind the positions (decrypt local ElGamal layer and then add another
//       layer of deterministic pohlig_hellman encryption.
//    2. re-randomize keys and counts.
//    3. shuffle all registers.
absl::StatusOr<CompleteExecutionPhaseOneResponse> CompleteExecutionPhaseOne(
    const CompleteExecutionPhaseOneRequest& request);

//  Complete work in the execution phase one at the aggregator worker.
//  More specifically, the worker would
//    1. decrypt the local ElGamal encryption on the positions.
//    2. join the registers by positions.
//    3. run sameKeyAggregation on the keys and counts.
//    4. add local (flag, count) noises (if configured to).
absl::StatusOr<CompleteExecutionPhaseOneAtAggregatorResponse>
CompleteExecutionPhaseOneAtAggregator(
    const CompleteExecutionPhaseOneAtAggregatorRequest& request);

//  Complete work in the execution phase two at a non-aggregator worker.
//  More specifically, the worker would
//    1. decrypt the local ElGamal encryption on the flags.
//    2. re-randomize the counts.
//    3. add local (flag_a, flag_b, count) noises (if configured to), while the
//    flags are encrypted with a partial composite ElGamal public key and the
//    counts are encrypted with the full composite ElGamal public key.
//    4. shuffle all the (flag_a, flag_b, count) tuples.
absl::StatusOr<CompleteExecutionPhaseTwoResponse> CompleteExecutionPhaseTwo(
    const CompleteExecutionPhaseTwoRequest& request);

//  Complete work in the execution phase two at the aggregator worker.
//  More specifically, the worker would
//    1. decrypt the local ElGamal encryption on the flags.
//    2. filter out non-desired noise and estimate the reach.
//    3. discard all destroyed (flag, count) tuples.
//    4. create the 2-D SameKeyAggregator (SKA) matrix.
absl::StatusOr<CompleteExecutionPhaseTwoAtAggregatorResponse>
CompleteExecutionPhaseTwoAtAggregator(
    const CompleteExecutionPhaseTwoAtAggregatorRequest& request);

//  Complete work in the execution phase three at a non-aggregator worker.
//  More specifically, the worker would
//    1. decrypt the the local ElGamal encryption on the SameKeyAggregator (SKA)
//    matrix.
absl::StatusOr<CompleteExecutionPhaseThreeResponse> CompleteExecutionPhaseThree(
    const CompleteExecutionPhaseThreeRequest& request);

//  Complete work in the execution phase three at the aggregator worker.
//  More specifically, the worker would
//    1. decrypt the the local ElGamal encryption on the SameKeyAggregator (SKA)
//    matrix.
//    2. estimate the frequency.
absl::StatusOr<CompleteExecutionPhaseThreeAtAggregatorResponse>
CompleteExecutionPhaseThreeAtAggregator(
    const CompleteExecutionPhaseThreeAtAggregatorRequest& request);

}  // namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2

#endif  // SRC_MAIN_CC_WFA_MEASUREMENT_INTERNAL_DUCHY_PROTOCOL_LIQUID_LEGIONS_V2_LIQUID_LEGIONS_V2_ENCRYPTION_UTILITY_H_
