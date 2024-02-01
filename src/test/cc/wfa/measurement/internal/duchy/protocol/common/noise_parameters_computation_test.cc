// Copyright 2021 The Cross-Media Measurement Authors
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

#include "gtest/gtest.h"
#include "math/distributed_discrete_gaussian_noiser.h"
#include "math/distributed_geometric_noiser.h"
#include "wfa/measurement/internal/duchy/noise_mechanism.pb.h"
#include "wfa/measurement/internal/duchy/protocol/liquid_legions_v2_noise_config.pb.h"

namespace wfa::measurement::internal::duchy::protocol::common {
namespace {

using ::wfa::measurement::internal::duchy::DifferentialPrivacyParams;
using ::wfa::measurement::internal::duchy::NoiseMechanism;
using ::wfa::measurement::internal::duchy::protocol::LiquidLegionsV2NoiseConfig;

TEST(GetBlindHistogramNoiser, GeometricOptionsResultShouldBeCorrect) {
  DifferentialPrivacyParams test_params;
  test_params.set_epsilon(std::log(3) / 10);
  test_params.set_delta(0.2 / 100000);
  int uncorrupted_party_count = 2;

  auto noiser = GetBlindHistogramNoiser(test_params, uncorrupted_party_count,
                                        NoiseMechanism::GEOMETRIC);
  const auto& options = noiser->options();

  EXPECT_EQ(options.contributor_count, uncorrupted_party_count);
  EXPECT_EQ(options.shift_offset, 291);
  EXPECT_EQ(options.truncate_threshold, 291);
}

TEST(GetBlindHistogramNoiser, GaussianOptionsResultShouldBeCorrect) {
  DifferentialPrivacyParams dp_params;
  dp_params.set_epsilon(std::log(3) / 10);
  dp_params.set_delta(0.2 / 100000);
  int uncorrupted_party_count = 2;

  auto noiser = GetBlindHistogramNoiser(dp_params, uncorrupted_party_count,
                                        NoiseMechanism::DISCRETE_GAUSSIAN);
  const auto& options = noiser->options();

  EXPECT_EQ(options.contributor_count, uncorrupted_party_count);
  EXPECT_EQ(options.shift_offset, 189);
  EXPECT_EQ(options.truncate_threshold, 189);
}

TEST(GetPublisherNoiser, GeometricOptionsExampleResultShouldBeCorrect) {
  DifferentialPrivacyParams test_params;
  test_params.set_epsilon(std::log(3) / 10);
  test_params.set_delta(0.2 / 100000);
  int publisher_count = 3;
  int uncorrupted_party_count = 2;

  auto noiser =
      GetPublisherNoiser(test_params, publisher_count, uncorrupted_party_count,
                         NoiseMechanism::GEOMETRIC);
  const auto& options = noiser->options();

  EXPECT_EQ(options.contributor_count, uncorrupted_party_count);
  EXPECT_EQ(options.shift_offset, 447);
  EXPECT_EQ(options.truncate_threshold, 447);
}

TEST(GetPublisherNoiser, GaussianOptionsExampleResultShouldBeCorrect) {
  DifferentialPrivacyParams test_params;
  test_params.set_epsilon(std::log(3) / 10);
  test_params.set_delta(0.2 / 100000);
  int publisher_count = 3;
  int uncorrupted_party_count = 4;

  auto noiser =
      GetPublisherNoiser(test_params, publisher_count, uncorrupted_party_count,
                         NoiseMechanism::DISCRETE_GAUSSIAN);
  const auto& options = noiser->options();

  EXPECT_EQ(options.contributor_count, uncorrupted_party_count);
  EXPECT_EQ(options.shift_offset, 137);
  EXPECT_EQ(options.truncate_threshold, 137);
}

TEST(GetGlobalReachDpNoiser, GeometricOptionsExampleResultShouldBeCorrect) {
  wfa::measurement::internal::duchy::DifferentialPrivacyParams test_params;
  test_params.set_epsilon(0.35 * std::log(3));
  test_params.set_delta(0.2 / 100000);
  int uncorrupted_party_count = 2;

  auto noiser = GetGlobalReachDpNoiser(test_params, uncorrupted_party_count,
                                       NoiseMechanism::GEOMETRIC);
  const auto& options = noiser->options();

  EXPECT_EQ(options.contributor_count, uncorrupted_party_count);
  EXPECT_EQ(options.shift_offset, 41);
  EXPECT_EQ(options.truncate_threshold, 41);
}

TEST(GetGlobalReachDpNoiser, GaussianOptionsExampleResultShouldBeCorrect) {
  wfa::measurement::internal::duchy::DifferentialPrivacyParams test_params;
  test_params.set_epsilon(0.35 * std::log(3));
  test_params.set_delta(0.2 / 100000);
  int uncorrupted_party_count = 2;

  auto noiser = GetGlobalReachDpNoiser(test_params, uncorrupted_party_count,
                                       NoiseMechanism::DISCRETE_GAUSSIAN);
  const auto& options = noiser->options();

  EXPECT_EQ(options.contributor_count, uncorrupted_party_count);
  EXPECT_EQ(options.shift_offset, 55);
  EXPECT_EQ(options.truncate_threshold, 55);
}

TEST(GetFrequencyNoiser, GeometricOptionsExampleResultShouldBeCorrect) {
  wfa::measurement::internal::duchy::DifferentialPrivacyParams test_params;
  test_params.set_epsilon(0.35 * std::log(3));
  test_params.set_delta(0.2 / 100000);
  int uncorrupted_party_count = 2;

  auto noiser = GetFrequencyNoiser(test_params, uncorrupted_party_count,
                                   NoiseMechanism::GEOMETRIC);
  const auto& options = noiser->options();

  EXPECT_EQ(options.contributor_count, uncorrupted_party_count);
  EXPECT_EQ(options.shift_offset, 84);
  EXPECT_EQ(options.truncate_threshold, 84);
}

TEST(GetFrequencyNoiser, GaussianOptionsExampleResultShouldBeCorrect) {
  wfa::measurement::internal::duchy::DifferentialPrivacyParams test_params;
  test_params.set_epsilon(0.35 * std::log(3));
  test_params.set_delta(0.2 / 100000);
  int uncorrupted_party_count = 3;

  auto noiser = GetFrequencyNoiser(test_params, uncorrupted_party_count,
                                   NoiseMechanism::DISCRETE_GAUSSIAN);
  const auto& options = noiser->options();

  EXPECT_EQ(options.contributor_count, uncorrupted_party_count);
  EXPECT_EQ(options.shift_offset, 45);
  EXPECT_EQ(options.truncate_threshold, 45);
}

}  // namespace
}  // namespace wfa::measurement::internal::duchy::protocol::common
