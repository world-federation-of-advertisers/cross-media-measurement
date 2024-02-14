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

#ifndef SRC_MAIN_CC_WFA_MEASUREMENT_INTERNAL_DUCHY_PROTOCOL_COMMON_NOISE_PARAMETERS_COMPUTATION_H_
#define SRC_MAIN_CC_WFA_MEASUREMENT_INTERNAL_DUCHY_PROTOCOL_COMMON_NOISE_PARAMETERS_COMPUTATION_H_

#include <memory>

#include "math/distributed_noiser.h"
#include "wfa/measurement/internal/duchy/differential_privacy.pb.h"
#include "wfa/measurement/internal/duchy/noise_mechanism.pb.h"

namespace wfa::measurement::internal::duchy::protocol::common {

using ::wfa::measurement::internal::duchy::NoiseMechanism;

std::unique_ptr<math::DistributedNoiser> GetBlindHistogramNoiser(
    const wfa::measurement::internal::duchy::DifferentialPrivacyParams& params,
    int uncorrupted_party_count, NoiseMechanism noise_mechanism);

std::unique_ptr<math::DistributedNoiser> GetPublisherNoiser(
    const wfa::measurement::internal::duchy::DifferentialPrivacyParams& params,
    int publisher_count, int uncorrupted_party_count,
    NoiseMechanism noise_mechanism);

std::unique_ptr<math::DistributedNoiser> GetGlobalReachDpNoiser(
    const wfa::measurement::internal::duchy::DifferentialPrivacyParams& params,
    int uncorrupted_party_count, NoiseMechanism noise_mechanism);

std::unique_ptr<math::DistributedNoiser> GetFrequencyNoiser(
    const wfa::measurement::internal::duchy::DifferentialPrivacyParams& params,
    int uncorrupted_party_count, NoiseMechanism noise_mechanism);

}  // namespace wfa::measurement::internal::duchy::protocol::common

#endif  // SRC_MAIN_CC_WFA_MEASUREMENT_INTERNAL_DUCHY_PROTOCOL_COMMON_NOISE_PARAMETERS_COMPUTATION_H_
