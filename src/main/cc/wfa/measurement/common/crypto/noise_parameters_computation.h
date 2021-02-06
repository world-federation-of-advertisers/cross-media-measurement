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

#ifndef WFA_MEASUREMENT_COMMON_CRYPTO_NOISE_PARAMETERS_COMPUTATION_H_
#define WFA_MEASUREMENT_COMMON_CRYPTO_NOISE_PARAMETERS_COMPUTATION_H_

#include "wfa/measurement/common/crypto/parameters.pb.h"
#include "wfa/measurement/common/math/distributions.h"

namespace wfa::measurement::common::crypto {

math::DistributedGeometricRandomComponentOptions GetBlindHistogramNoiseOptions(
    const DifferentialPrivacyParams& params, int publisher_count,
    int uncorrupted_party_count);

math::DistributedGeometricRandomComponentOptions
GetNoiseForPublisherNoiseOptions(const DifferentialPrivacyParams& params,
                                 int publisher_count,
                                 int uncorrupted_party_count);

math::DistributedGeometricRandomComponentOptions GetGlobalReachDpNoiseOptions(
    const DifferentialPrivacyParams& params, int uncorrupted_party_count);

math::DistributedGeometricRandomComponentOptions GetFrequencyNoiseOptions(
    const DifferentialPrivacyParams& params, int max_frequency,
    int uncorrupted_party_count);

math::TruncatedDiscreteLaplaceDistributedOptions GetPublisherNoiseOptions(
    const DifferentialPrivacyParams& params, int publisher_count);

}  // namespace wfa::measurement::common::crypto

#endif  // WFA_MEASUREMENT_COMMON_CRYPTO_NOISE_PARAMETERS_COMPUTATION_H_