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

#ifndef SRC_MAIN_CC_WFA_MEASUREMENT_INTERNAL_DUCHY_PROTOCOL_LIQUID_LEGIONS_V2_NOISE_PARAMETERS_COMPUTATION_H_
#define SRC_MAIN_CC_WFA_MEASUREMENT_INTERNAL_DUCHY_PROTOCOL_LIQUID_LEGIONS_V2_NOISE_PARAMETERS_COMPUTATION_H_

#include "math/distributed_geometric_noiser.h"
#include "math/distributed_noiser.h"
#include "wfa/measurement/internal/duchy/differential_privacy.pb.h"

namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2 {

math::DistributedGeometricNoiseComponentOptions
GetBlindHistogramGeometricNoiseOptions(
    const wfa::measurement::internal::duchy::DifferentialPrivacyParams& params,
    int uncorrupted_party_count);

math::DistributedGeometricNoiseComponentOptions
GetNoiseForPublisherGeometricNoiseOptions(
    const wfa::measurement::internal::duchy::DifferentialPrivacyParams& params,
    int publisher_count, int uncorrupted_party_count);

math::DistributedGeometricNoiseComponentOptions
GetGlobalReachDpGeometricNoiseOptions(
    const wfa::measurement::internal::duchy::DifferentialPrivacyParams& params,
    int uncorrupted_party_count);

math::DistributedGeometricNoiseComponentOptions
GetFrequencyGeometricNoiseOptions(
    const wfa::measurement::internal::duchy::DifferentialPrivacyParams& params,
    int uncorrupted_party_count);

}  // namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2

#endif  // SRC_MAIN_CC_WFA_MEASUREMENT_INTERNAL_DUCHY_PROTOCOL_LIQUID_LEGIONS_V2_NOISE_PARAMETERS_COMPUTATION_H_
