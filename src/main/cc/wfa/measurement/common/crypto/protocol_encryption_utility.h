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

#ifndef WFA_MEASUREMENT_COMMON_CRYPTO_PROTOCOL_ENCRYPTION_UTILITY_H_
#define WFA_MEASUREMENT_COMMON_CRYPTO_PROTOCOL_ENCRYPTION_UTILITY_H_

#include "util/statusor.h"
#include "wfa/measurement/internal/duchy/protocol_encryption_methods.pb.h"

namespace wfa {
namespace measurement {
namespace common {
namespace crypto {

using ::private_join_and_compute::StatusOr;
using ::wfa::measurement::internal::duchy::AddNoiseToSketchRequest;
using ::wfa::measurement::internal::duchy::AddNoiseToSketchResponse;
using ::wfa::measurement::internal::duchy::
    BlindLastLayerIndexThenJoinRegistersRequest;
using ::wfa::measurement::internal::duchy::
    BlindLastLayerIndexThenJoinRegistersResponse;
using ::wfa::measurement::internal::duchy::BlindOneLayerRegisterIndexRequest;
using ::wfa::measurement::internal::duchy::BlindOneLayerRegisterIndexResponse;
using ::wfa::measurement::internal::duchy::DecryptLastLayerFlagAndCountRequest;
using ::wfa::measurement::internal::duchy::DecryptLastLayerFlagAndCountResponse;
using ::wfa::measurement::internal::duchy::DecryptOneLayerFlagAndCountRequest;
using ::wfa::measurement::internal::duchy::DecryptOneLayerFlagAndCountResponse;

// Add noise registers to the input sketch.
StatusOr<AddNoiseToSketchResponse> AddNoiseToSketch(
    const AddNoiseToSketchRequest& request);

// Blind (one layer) all register indexes of a sketch. Only 3-tuple
// (register_index, fingerprint, count) registers are supported.
StatusOr<BlindOneLayerRegisterIndexResponse> BlindOneLayerRegisterIndex(
    const BlindOneLayerRegisterIndexRequest& request);

// Blind (last layer) the register indexes, and then join the registers by the
// deterministically encrypted register indexes, and then merge the counts
// using the same-key-aggregating algorithm.
StatusOr<BlindLastLayerIndexThenJoinRegistersResponse>
BlindLastLayerIndexThenJoinRegisters(
    const BlindLastLayerIndexThenJoinRegistersRequest& request);

// Decrypt (one layer) the count and flag of all registers.
StatusOr<DecryptOneLayerFlagAndCountResponse> DecryptOneLayerFlagAndCount(
    const DecryptOneLayerFlagAndCountRequest& request);

// Decrypt (last layer) the count and flag of all registers.
StatusOr<DecryptLastLayerFlagAndCountResponse> DecryptLastLayerFlagAndCount(
    const DecryptLastLayerFlagAndCountRequest& request);

}  // namespace crypto
}  // namespace common
}  // namespace measurement
}  // namespace wfa

#endif  // WFA_MEASUREMENT_COMMON_CRYPTO_PROTOCOL_ENCRYPTION_UTILITY_H_
