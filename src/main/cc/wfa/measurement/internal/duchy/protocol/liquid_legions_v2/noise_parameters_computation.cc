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

#include "wfa/measurement/internal/duchy/protocol/liquid_legions_v2/noise_parameters_computation.h"

namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2 {

namespace {

int ComputateMuPolya(double epsilon, double delta, int sensitivity, int n) {
  ABSL_ASSERT(epsilon > 0);
  ABSL_ASSERT(delta > 0);
  ABSL_ASSERT(sensitivity > 0);
  ABSL_ASSERT(n > 0);
  return std::ceil(
      std::log(2.0 * n * sensitivity * (1 + std::exp(epsilon)) / delta) /
      (epsilon / sensitivity));
}

}  // namespace

math::DistributedGeometricNoiseComponentOptions
GetBlindHistogramGeometricNoiseOptions(
    const wfa::measurement::internal::duchy::DifferentialPrivacyParams& params,
    int uncorrupted_party_count) {
  ABSL_ASSERT(uncorrupted_party_count > 0);
  double success_ratio = std::exp(-params.epsilon() / 2);
  int offset = ComputateMuPolya(params.epsilon(), params.delta(), 2,
                                uncorrupted_party_count);
  return {
      .contributor_count = uncorrupted_party_count,
      .p = success_ratio,
      .truncate_threshold = offset,
      .shift_offset = offset,
  };
}

math::DistributedGeometricNoiseComponentOptions
GetNoiseForPublisherGeometricNoiseOptions(
    const wfa::measurement::internal::duchy::DifferentialPrivacyParams& params,
    int publisher_count, int uncorrupted_party_count) {
  ABSL_ASSERT(publisher_count > 0);
  ABSL_ASSERT(uncorrupted_party_count > 0);
  double success_ratio = std::exp(-params.epsilon() / publisher_count);
  int offset = ComputateMuPolya(params.epsilon(), params.delta(),
                                publisher_count, uncorrupted_party_count);
  return {
      .contributor_count = uncorrupted_party_count,
      .p = success_ratio,
      .truncate_threshold = offset,
      .shift_offset = offset,
  };
}

math::DistributedGeometricNoiseComponentOptions
GetGlobalReachDpGeometricNoiseOptions(
    const wfa::measurement::internal::duchy::DifferentialPrivacyParams& params,
    int uncorrupted_party_count) {
  ABSL_ASSERT(uncorrupted_party_count > 0);
  double success_ratio = std::exp(-params.epsilon());
  int offset = ComputateMuPolya(params.epsilon(), params.delta(), 1,
                                uncorrupted_party_count);
  return {
      .contributor_count = uncorrupted_party_count,
      .p = success_ratio,
      .truncate_threshold = offset,
      .shift_offset = offset,
  };
}

math::DistributedGeometricNoiseComponentOptions
GetFrequencyGeometricNoiseOptions(
    const wfa::measurement::internal::duchy::DifferentialPrivacyParams& params,
    int uncorrupted_party_count) {
  ABSL_ASSERT(uncorrupted_party_count > 0);
  double success_ratio = std::exp(-params.epsilon() / 2);
  int offset = ComputateMuPolya(params.epsilon(), params.delta(), 2,
                                uncorrupted_party_count);
  return {
      .contributor_count = uncorrupted_party_count,
      .p = success_ratio,
      .truncate_threshold = offset,
      .shift_offset = offset,
  };
}

}  // namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2
