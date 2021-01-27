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

#include "wfa/measurement/common/crypto/noise_parameters_computation.h"

namespace wfa::measurement::common::crypto {

namespace {

int computate_mu_polya(double epsilon, double delta, int n) {
  ABSL_ASSERT(epsilon > 0);
  ABSL_ASSERT(delta > 0);
  ABSL_ASSERT(n > 0);
  return std::ceil(std::log(2.0 * n / (delta * (1 - std::exp(-epsilon)))) /
                   epsilon);
}

}  // namespace

math::DistributedGeometricRandomComponentOptions GetBlindHistogramNoiseOptions(
    const DifferentialPrivacyParams& params, int publisher_count,
    int uncorrupted_party_count) {
  ABSL_ASSERT(publisher_count > 0);
  ABSL_ASSERT(uncorrupted_party_count > 0);
  double success_ratio = std::exp(-params.epsilon() / 3);
  int offset =
      computate_mu_polya(params.epsilon() / 3, params.delta(),
                         2 * uncorrupted_party_count * publisher_count);
  return {
      .num = uncorrupted_party_count,
      .p = success_ratio,
      .truncate_threshold = offset,
      .shift_offset = offset,
  };
}

math::DistributedGeometricRandomComponentOptions
GetNoiseForPublisherNoiseOptions(const DifferentialPrivacyParams& params,
                                 int publisher_count,
                                 int uncorrupted_party_count) {
  ABSL_ASSERT(publisher_count > 0);
  ABSL_ASSERT(uncorrupted_party_count > 0);
  double success_ratio = std::exp(-params.epsilon() / publisher_count);
  int offset = computate_mu_polya(params.epsilon() / publisher_count,
                                  params.delta(), uncorrupted_party_count);
  return {
      .num = uncorrupted_party_count,
      .p = success_ratio,
      .truncate_threshold = offset,
      .shift_offset = offset,
  };
}

math::DistributedGeometricRandomComponentOptions GetGlobalReachDpNoiseOptions(
    const DifferentialPrivacyParams& params, int uncorrupted_party_count) {
  ABSL_ASSERT(uncorrupted_party_count > 0);
  double success_ratio = std::exp(-params.epsilon());
  int offset = computate_mu_polya(params.epsilon(), params.delta(),
                                  uncorrupted_party_count);
  return {
      .num = uncorrupted_party_count,
      .p = success_ratio,
      .truncate_threshold = offset,
      .shift_offset = offset,
  };
}

math::DistributedGeometricRandomComponentOptions GetFrequencyNoiseOptions(
    const DifferentialPrivacyParams& params, int max_frequency,
    int uncorrupted_party_count) {
  ABSL_ASSERT(max_frequency > 0);
  ABSL_ASSERT(uncorrupted_party_count > 0);
  double success_ratio = std::exp(-params.epsilon() / 2);
  int offset = computate_mu_polya(params.epsilon() / 2, params.delta(),
                                  2 * uncorrupted_party_count * max_frequency);
  return {
      .num = uncorrupted_party_count,
      .p = success_ratio,
      .truncate_threshold = offset,
      .shift_offset = offset,
  };
}

}  // namespace wfa::measurement::common::crypto