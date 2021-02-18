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

#ifndef SRC_MAIN_CC_WFA_MEASUREMENT_PROTOCOL_LIQUID_LEGIONS_V1_LIQUID_LEGIONS_V1_ENCRYPTION_UTILITY_H_
#define SRC_MAIN_CC_WFA_MEASUREMENT_PROTOCOL_LIQUID_LEGIONS_V1_LIQUID_LEGIONS_V1_ENCRYPTION_UTILITY_H_

#include "absl/status/statusor.h"
#include "wfa/measurement/protocol/crypto/liquid_legions_v1_encryption_methods.pb.h"

namespace wfa::measurement::protocol::liquid_legions_v1 {

using ::wfa::measurement::protocol::AddNoiseToSketchRequest;
using ::wfa::measurement::protocol::AddNoiseToSketchResponse;
using ::wfa::measurement::protocol::BlindLastLayerIndexThenJoinRegistersRequest;
using ::wfa::measurement::protocol::
    BlindLastLayerIndexThenJoinRegistersResponse;
using ::wfa::measurement::protocol::BlindOneLayerRegisterIndexRequest;
using ::wfa::measurement::protocol::BlindOneLayerRegisterIndexResponse;
using ::wfa::measurement::protocol::DecryptLastLayerFlagAndCountRequest;
using ::wfa::measurement::protocol::DecryptLastLayerFlagAndCountResponse;
using ::wfa::measurement::protocol::DecryptOneLayerFlagAndCountRequest;
using ::wfa::measurement::protocol::DecryptOneLayerFlagAndCountResponse;

// Add noise registers to the input sketch.
absl::StatusOr<AddNoiseToSketchResponse> AddNoiseToSketch(
    const AddNoiseToSketchRequest& request);

// Blind (one layer) all register indexes of a sketch. Only 3-tuple
// (register_index, fingerprint, count) registers are supported.
absl::StatusOr<BlindOneLayerRegisterIndexResponse> BlindOneLayerRegisterIndex(
    const BlindOneLayerRegisterIndexRequest& request);

// Blind (last layer) the register indexes, and then join the registers by the
// deterministically encrypted register indexes, and then merge the counts
// using the same-key-aggregating algorithm.
absl::StatusOr<BlindLastLayerIndexThenJoinRegistersResponse>
BlindLastLayerIndexThenJoinRegisters(
    const BlindLastLayerIndexThenJoinRegistersRequest& request);

// Decrypt (one layer) the count and flag of all registers.
absl::StatusOr<DecryptOneLayerFlagAndCountResponse> DecryptOneLayerFlagAndCount(
    const DecryptOneLayerFlagAndCountRequest& request);

// Decrypt (last layer) the count and flag of all registers.
absl::StatusOr<DecryptLastLayerFlagAndCountResponse>
DecryptLastLayerFlagAndCount(
    const DecryptLastLayerFlagAndCountRequest& request);

}  // namespace wfa::measurement::protocol::liquid_legions_v1

#endif  // SRC_MAIN_CC_WFA_MEASUREMENT_PROTOCOL_LIQUID_LEGIONS_V1_LIQUID_LEGIONS_V1_ENCRYPTION_UTILITY_H_
