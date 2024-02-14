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

#include "wfa/measurement/internal/duchy/protocol/common/noise_parameters_computation.h"

#include <utility>

#include "math/distributed_discrete_gaussian_noiser.h"
#include "math/distributed_geometric_noiser.h"

namespace wfa::measurement::internal::duchy::protocol::common {

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

math::DistributedGeometricNoiseComponentOptions GetGeometricNoiseOptions(
    const wfa::measurement::internal::duchy::DifferentialPrivacyParams& params,
    int publisher_count, int uncorrupted_party_count) {
  ABSL_ASSERT(uncorrupted_party_count > 0);
  double success_ratio = std::exp(-params.epsilon() / publisher_count);
  int offset = ComputateMuPolya(params.epsilon(), params.delta(),
                                publisher_count, uncorrupted_party_count);
  return math::DistributedGeometricNoiseComponentOptions{
      uncorrupted_party_count, success_ratio, offset, offset};
}

int ComputeMuDiscreteGaussian(double epsilon, double delta,
                              double sigma_distributed,
                              int64_t uncorrupted_party_count) {
  ABSL_ASSERT(epsilon > 0);
  ABSL_ASSERT(delta > 0);
  ABSL_ASSERT(uncorrupted_party_count > 0);

  // The sum of delta1 and delta2 should be delta.
  // In practice, set delta1 = delta2 = 0.5 * delta for simplicity.
  double delta2 = 0.5 * delta;

  return std::ceil(sigma_distributed *
                   std::sqrt(2 * std::log(uncorrupted_party_count *
                                          (1 + std::exp(epsilon)) / delta2)));
}

math::DistributedDiscreteGaussianNoiseComponentOptions
GetDiscreteGaussianNoiseOptions(
    const wfa::measurement::internal::duchy::DifferentialPrivacyParams& params,
    int64_t uncorrupted_party_count) {
  double epsilon = params.epsilon();
  double delta = params.delta();

  ABSL_ASSERT(epsilon > 0);
  ABSL_ASSERT(delta > 0);

  // The sum of delta1 and delta2 should be delta.
  // In practice, set delta1 = delta2 = 0.5 * delta for simplicity.
  double delta1 = 0.5 * delta;
  double sigma = std::sqrt(2 * std::log(1.25 / delta1)) / epsilon;
  // This simple formula to derive sigma_distributed is valid only for
  // continuous Gaussian and is used as an approximation here.
  double sigma_distributed = sigma / sqrt(uncorrupted_party_count);
  int offset =
      ComputeMuDiscreteGaussian(params.epsilon(), params.delta(),
                                sigma_distributed, uncorrupted_party_count);

  return math::DistributedDiscreteGaussianNoiseComponentOptions{
      uncorrupted_party_count, sigma_distributed, offset, offset};
}

}  // namespace

std::unique_ptr<math::DistributedNoiser> GetBlindHistogramNoiser(
    const wfa::measurement::internal::duchy::DifferentialPrivacyParams& params,
    int uncorrupted_party_count, NoiseMechanism noise_mechanism) {
  ABSL_ASSERT(noise_mechanism == NoiseMechanism::DISCRETE_GAUSSIAN ||
              noise_mechanism == NoiseMechanism::GEOMETRIC);

  if (noise_mechanism == NoiseMechanism::GEOMETRIC) {
    auto noiseOptions =
        GetGeometricNoiseOptions(params, 2, uncorrupted_party_count);
    return std::make_unique<math::DistributedGeometricNoiser>(noiseOptions);
  } else {
    // noise_mechanism == NoiseMechanism::DISCRETE_GAUSSIAN
    auto noiseOptions =
        GetDiscreteGaussianNoiseOptions(params, uncorrupted_party_count);
    return std::make_unique<math::DistributedDiscreteGaussianNoiser>(
        noiseOptions);
  }
}

std::unique_ptr<math::DistributedNoiser> GetPublisherNoiser(
    const wfa::measurement::internal::duchy::DifferentialPrivacyParams& params,
    int publisher_count, int uncorrupted_party_count,
    NoiseMechanism noise_mechanism) {
  ABSL_ASSERT(noise_mechanism == NoiseMechanism::DISCRETE_GAUSSIAN ||
              noise_mechanism == NoiseMechanism::GEOMETRIC);

  if (noise_mechanism == NoiseMechanism::GEOMETRIC) {
    auto noiseOptions = GetGeometricNoiseOptions(params, publisher_count,
                                                 uncorrupted_party_count);
    return std::make_unique<math::DistributedGeometricNoiser>(noiseOptions);
  } else {
    // noise_mechanism == NoiseMechanism::DISCRETE_GAUSSIAN
    auto noiseOptions =
        GetDiscreteGaussianNoiseOptions(params, uncorrupted_party_count);
    return std::make_unique<math::DistributedDiscreteGaussianNoiser>(
        noiseOptions);
  }
}

std::unique_ptr<math::DistributedNoiser> GetGlobalReachDpNoiser(
    const wfa::measurement::internal::duchy::DifferentialPrivacyParams& params,
    int uncorrupted_party_count, NoiseMechanism noise_mechanism) {
  ABSL_ASSERT(noise_mechanism == NoiseMechanism::DISCRETE_GAUSSIAN ||
              noise_mechanism == NoiseMechanism::GEOMETRIC);

  if (noise_mechanism == NoiseMechanism::GEOMETRIC) {
    auto noiseOptions =
        GetGeometricNoiseOptions(params, 1, uncorrupted_party_count);
    return std::make_unique<math::DistributedGeometricNoiser>(noiseOptions);
  } else {
    // noise_mechanism == NoiseMechanism::DISCRETE_GAUSSIAN
    auto noiseOptions =
        GetDiscreteGaussianNoiseOptions(params, uncorrupted_party_count);
    return std::make_unique<math::DistributedDiscreteGaussianNoiser>(
        noiseOptions);
  }
}

std::unique_ptr<math::DistributedNoiser> GetFrequencyNoiser(
    const wfa::measurement::internal::duchy::DifferentialPrivacyParams& params,
    int uncorrupted_party_count, NoiseMechanism noise_mechanism) {
  ABSL_ASSERT(noise_mechanism == NoiseMechanism::DISCRETE_GAUSSIAN ||
              noise_mechanism == NoiseMechanism::GEOMETRIC);

  if (noise_mechanism == NoiseMechanism::GEOMETRIC) {
    auto noiseOptions =
        GetGeometricNoiseOptions(params, 2, uncorrupted_party_count);
    return std::make_unique<math::DistributedGeometricNoiser>(noiseOptions);
  }
  {
    // noise_mechanism == NoiseMechanism::DISCRETE_GAUSSIAN
    auto noiseOptions =
        GetDiscreteGaussianNoiseOptions(params, uncorrupted_party_count);
    return std::make_unique<math::DistributedDiscreteGaussianNoiser>(
        noiseOptions);
  }
}

}  // namespace wfa::measurement::internal::duchy::protocol::common
