// Copyright 2020 The Measurement System Authors
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

#ifndef WFA_MEASUREMENT_COMMON_CRYPTO_LIQUID_LEGIONS_V2_ENCRYPTION_UTILITY_H_
#define WFA_MEASUREMENT_COMMON_CRYPTO_LIQUID_LEGIONS_V2_ENCRYPTION_UTILITY_H_

#include "absl/status/statusor.h"
#include "wfa/measurement/common/crypto/liquid_legions_v2_encryption_methods.pb.h"

namespace wfa::measurement::common::crypto {

// Complete work in the setup phase at both the aggregator and non-aggregator
// workers. More specifically, the worker would
//   1. add local noise registers (if configured to).
//   2. shuffle all registers.
absl::StatusOr<CompleteSetupPhaseResponse> CompleteSetupPhase(
    const CompleteSetupPhaseRequest& request);

//  Complete work in the reach estimation phase at a non-aggregator worker.
//  More specifically, the worker would
//    1. blind the positions (decrypt local ElGamal layer and then add another
//       layer of deterministic pohlig_hellman encryption.
//    2. re-randomize keys and counts.
//    3. shuffle all registers.
absl::StatusOr<CompleteReachEstimationPhaseResponse>
CompleteReachEstimationPhase(
    const CompleteReachEstimationPhaseRequest& request);

//  Complete work in the reach estimation phase at the aggregator worker.
//  More specifically, the worker would
//    1. decrypt the local ElGamal encryption on the positions.
//    2. join the registers by positions.
//    3. run sameKeyAggregation on the keys and counts.
//    4. estimate the reach.
//    5. add local (flag, count) noises (if configured to).
absl::StatusOr<CompleteReachEstimationPhaseAtAggregatorResponse>
CompleteReachEstimationPhaseAtAggregator(
    const CompleteReachEstimationPhaseAtAggregatorRequest& request);

//  Complete work in the filtering phase at a non-aggregator worker.
//  More specifically, the worker would
//    1. decrypt the local ElGamal encryption on the flags.
//    2. add local (flag, count) noises (if configured to), while the flags are
//    encrypted with a partial composite ElGamal public key and the counts are
//    encrypted with the full composite ElGamal public key.
absl::StatusOr<CompleteFilteringPhaseResponse> CompleteFilteringPhase(
    const CompleteFilteringPhaseRequest& request);

//  Complete work in the filtering phase at the aggregator worker.
//  More specifically, the worker would
//    1. decrypt the local ElGamal encryption on the flags.
//    2. discard all destroyed (flag, count) tuples.
//    3. create the 2-D SameKeyAggregator (SKA) matrix.
absl::StatusOr<CompleteFilteringPhaseAtAggregatorResponse>
CompleteFilteringPhaseAtAggregator(
    const CompleteFilteringPhaseAtAggregatorRequest& request);

//  Complete work in the frequency estimation phase at a non-aggregator worker.
//  More specifically, the worker would
//    1. decrypt the the local ElGamal encryption on the SameKeyAggregator (SKA)
//    matrix.
absl::StatusOr<CompleteFrequencyEstimationPhaseResponse>
CompleteFrequencyEstimationPhase(
    const CompleteFrequencyEstimationPhaseRequest& request);

//  Complete work in the frequency estimation phase at the aggregator worker.
//  More specifically, the worker would
//    1. decrypt the the local ElGamal encryption on the SameKeyAggregator (SKA)
//    matrix.
//    2. estimate the frequency.
absl::StatusOr<CompleteFrequencyEstimationPhaseAtAggregatorResponse>
CompleteFrequencyEstimationPhaseAtAggregator(
    const CompleteFrequencyEstimationPhaseAtAggregatorRequest& request);

}  // namespace wfa::measurement::common::crypto

#endif  // WFA_MEASUREMENT_COMMON_CRYPTO_LIQUID_LEGIONS_V2_ENCRYPTION_UTILITY_H_
