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

#ifndef SRC_MAIN_CC_WFA_MEASUREMENT_INTERNAL_DUCHY_PROTOCOL_LIQUID_LEGIONS_V2_TESTING_LIQUID_LEGIONS_V2_ENCRYPTION_UTILITY_HELPER_H_
#define SRC_MAIN_CC_WFA_MEASUREMENT_INTERNAL_DUCHY_PROTOCOL_LIQUID_LEGIONS_V2_TESTING_LIQUID_LEGIONS_V2_ENCRYPTION_UTILITY_HELPER_H_

#include "absl/status/statusor.h"
#include "any_sketch/crypto/sketch_encrypter.h"
#include "wfa/measurement/internal/duchy/crypto.pb.h"
#include "wfa/measurement/internal/duchy/differential_privacy.pb.h"

namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2 {

using ::wfa::any_sketch::Sketch;
using ::wfa::measurement::internal::duchy::DifferentialPrivacyParams;
using ::wfa::measurement::internal::duchy::ElGamalPublicKey;

::wfa::any_sketch::crypto::ElGamalPublicKey ToAnySketchElGamalKey(
    ElGamalPublicKey key);

ElGamalPublicKey ToDuchyInternalElGamalKey(
    ::wfa::any_sketch::crypto::ElGamalPublicKey key);

Sketch CreateEmptyLiquidLegionsSketch();

Sketch CreateReachOnlyEmptyLiquidLegionsSketch();

DifferentialPrivacyParams MakeDifferentialPrivacyParams(double epsilon,
                                                        double delta);

}  // namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2

#endif  // SRC_MAIN_CC_WFA_MEASUREMENT_INTERNAL_DUCHY_PROTOCOL_LIQUID_LEGIONS_V2_TESTING_LIQUID_LEGIONS_V2_ENCRYPTION_UTILITY_HELPER_H_
