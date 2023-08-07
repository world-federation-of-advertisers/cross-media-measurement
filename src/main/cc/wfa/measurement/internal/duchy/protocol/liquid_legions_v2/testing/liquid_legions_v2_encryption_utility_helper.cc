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

#include "wfa/measurement/internal/duchy/protocol/liquid_legions_v2/testing/liquid_legions_v2_encryption_utility_helper.h"

#include "estimation/estimators.h"

namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2 {

using ::wfa::any_sketch::Sketch;
using ::wfa::any_sketch::SketchConfig;
using ::wfa::measurement::internal::duchy::ElGamalPublicKey;

::wfa::any_sketch::crypto::ElGamalPublicKey ToAnySketchElGamalKey(
    ElGamalPublicKey key) {
  ::wfa::any_sketch::crypto::ElGamalPublicKey result;
  result.set_generator(key.generator());
  result.set_element(key.element());
  return result;
}

ElGamalPublicKey ToDuchyInternalElGamalKey(
    ::wfa::any_sketch::crypto::ElGamalPublicKey key) {
  ElGamalPublicKey result;
  result.set_generator(key.generator());
  result.set_element(key.element());
  return result;
}

Sketch CreateEmptyLiquidLegionsSketch() {
  Sketch plain_sketch;
  plain_sketch.mutable_config()->add_values()->set_aggregator(
      SketchConfig::ValueSpec::UNIQUE);
  plain_sketch.mutable_config()->add_values()->set_aggregator(
      SketchConfig::ValueSpec::SUM);
  return plain_sketch;
}

Sketch CreateReachOnlyEmptyLiquidLegionsSketch() {
  Sketch plain_sketch;
  return plain_sketch;
}

DifferentialPrivacyParams MakeDifferentialPrivacyParams(double epsilon,
                                                        double delta) {
  DifferentialPrivacyParams params;
  params.set_epsilon(epsilon);
  params.set_delta(delta);
  return params;
}

}  // namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2
