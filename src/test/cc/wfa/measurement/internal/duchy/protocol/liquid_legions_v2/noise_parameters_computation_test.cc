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

#include "wfa/measurement/internal/duchy/protocol/liquid_legions_v2/noise_parameters_computation.h"

#include "gtest/gtest.h"
#include "math/distributed_discrete_gaussian_noiser.h"
#include "math/distributed_geometric_noiser.h"
#include "wfa/measurement/internal/duchy/protocol/liquid_legions_v2_noise_config.pb.h"

namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2 {
namespace {

using ::wfa::measurement::internal::duchy::DifferentialPrivacyParams;
using ::wfa::measurement::internal::duchy::protocol::LiquidLegionsV2NoiseConfig;

TEST(GetBlindHistogramNoiserAndOptions, GeometricOptionsResultShouldBeCorrect) {
  DifferentialPrivacyParams test_params;
  test_params.set_epsilon(std::log(3) / 10);
  test_params.set_delta(0.2 / 100000);
  int uncorrupted_party_count = 2;

  auto options =
      GetBlindHistogramNoiserAndOptions(test_params, uncorrupted_party_count,
                                        LiquidLegionsV2NoiseConfig::GEOMETRIC)
          .options;

  EXPECT_EQ(options->contributor_count, uncorrupted_party_count);
  EXPECT_EQ(options->shift_offset, 291);
  EXPECT_EQ(options->truncate_threshold, 291);
  auto geometricOptions =
      static_cast<math::DistributedGeometricNoiseComponentOptions*>(
          options.get());
  EXPECT_NEAR(geometricOptions->p, 0.947, 0.001);
}

TEST(GetBlindHistogramNoiserAndOptions, GaussianOptionsResultShouldBeCorrect) {
  DifferentialPrivacyParams dp_params;
  dp_params.set_epsilon(std::log(3) / 10);
  dp_params.set_delta(0.2 / 100000);
  int uncorrupted_party_count = 2;

  auto options = GetBlindHistogramNoiserAndOptions(
                     dp_params, uncorrupted_party_count,
                     LiquidLegionsV2NoiseConfig::DISCRETE_GAUSSIAN)
                     .options;

  EXPECT_EQ(options->contributor_count, uncorrupted_party_count);
  EXPECT_EQ(options->shift_offset, 189);
  EXPECT_EQ(options->truncate_threshold, 189);
  auto gaussianOptions =
      static_cast<math::DistributedDiscreteGaussianNoiseComponentOptions*>(
          options.get());
  EXPECT_NEAR(gaussianOptions->sigma_distributed, 34.105, 0.001);
}

TEST(GetPublisherNoiserAndOptions,
     GeometricOptionsExampleResultShouldBeCorrect) {
  DifferentialPrivacyParams test_params;
  test_params.set_epsilon(std::log(3) / 10);
  test_params.set_delta(0.2 / 100000);
  int publisher_count = 3;
  int uncorrupted_party_count = 2;

  auto options = GetPublisherNoiserAndOptions(
                     test_params, publisher_count, uncorrupted_party_count,
                     LiquidLegionsV2NoiseConfig::GEOMETRIC)
                     .options;

  EXPECT_EQ(options->contributor_count, uncorrupted_party_count);
  EXPECT_EQ(options->shift_offset, 447);
  EXPECT_EQ(options->truncate_threshold, 447);
  auto geometricOptions =
      static_cast<math::DistributedGeometricNoiseComponentOptions*>(
          options.get());
  EXPECT_NEAR(geometricOptions->p, 0.964, 0.001);
}

TEST(GetPublisherNoiserAndOptions,
     GaussianOptionsExampleResultShouldBeCorrect) {
  DifferentialPrivacyParams test_params;
  test_params.set_epsilon(std::log(3) / 10);
  test_params.set_delta(0.2 / 100000);
  int publisher_count = 3;
  int uncorrupted_party_count = 4;

  auto options = GetPublisherNoiserAndOptions(
                     test_params, publisher_count, uncorrupted_party_count,
                     LiquidLegionsV2NoiseConfig::DISCRETE_GAUSSIAN)
                     .options;

  EXPECT_EQ(options->contributor_count, uncorrupted_party_count);
  EXPECT_EQ(options->shift_offset, 137);
  EXPECT_EQ(options->truncate_threshold, 137);
  auto gaussianOptions =
      static_cast<math::DistributedDiscreteGaussianNoiseComponentOptions*>(
          options.get());
  EXPECT_NEAR(gaussianOptions->sigma_distributed, 24.115, 0.001);
}

TEST(GetGlobalReachDpNoiserAndOptions,
     GeometricOptionsExampleResultShouldBeCorrect) {
  wfa::measurement::internal::duchy::DifferentialPrivacyParams test_params;
  test_params.set_epsilon(0.35 * std::log(3));
  test_params.set_delta(0.2 / 100000);
  int uncorrupted_party_count = 2;

  auto options =
      GetGlobalReachDpNoiserAndOptions(test_params, uncorrupted_party_count,
                                       LiquidLegionsV2NoiseConfig::GEOMETRIC)
          .options;

  EXPECT_EQ(options->contributor_count, uncorrupted_party_count);
  EXPECT_EQ(options->shift_offset, 41);
  EXPECT_EQ(options->truncate_threshold, 41);
  auto geometricOptions =
      static_cast<math::DistributedGeometricNoiseComponentOptions*>(
          options.get());
  EXPECT_NEAR(geometricOptions->p, 0.681, 0.001);
}

TEST(GetGlobalReachDpNoiserAndOptions,
     GaussianOptionsExampleResultShouldBeCorrect) {
  wfa::measurement::internal::duchy::DifferentialPrivacyParams test_params;
  test_params.set_epsilon(0.35 * std::log(3));
  test_params.set_delta(0.2 / 100000);
  int uncorrupted_party_count = 2;

  auto options = GetGlobalReachDpNoiserAndOptions(
                     test_params, uncorrupted_party_count,
                     LiquidLegionsV2NoiseConfig::DISCRETE_GAUSSIAN)
                     .options;

  EXPECT_EQ(options->contributor_count, uncorrupted_party_count);
  EXPECT_EQ(options->shift_offset, 55);
  EXPECT_EQ(options->truncate_threshold, 55);
  auto gaussianOptions =
      static_cast<math::DistributedDiscreteGaussianNoiseComponentOptions*>(
          options.get());
  EXPECT_NEAR(gaussianOptions->sigma_distributed, 9.744, 0.001);
}

TEST(GetFrequencyNoiserAndOptions,
     GeometricOptionsExampleResultShouldBeCorrect) {
  wfa::measurement::internal::duchy::DifferentialPrivacyParams test_params;
  test_params.set_epsilon(0.35 * std::log(3));
  test_params.set_delta(0.2 / 100000);
  int uncorrupted_party_count = 2;

  auto options =
      GetFrequencyNoiserAndOptions(test_params, uncorrupted_party_count,
                                   LiquidLegionsV2NoiseConfig::GEOMETRIC)
          .options;

  EXPECT_EQ(options->contributor_count, uncorrupted_party_count);
  EXPECT_EQ(options->shift_offset, 84);
  EXPECT_EQ(options->truncate_threshold, 84);
  auto geometricOptions =
      static_cast<math::DistributedGeometricNoiseComponentOptions*>(
          options.get());
  EXPECT_NEAR(geometricOptions->p, 0.825, 0.001);
}

TEST(GetFrequencyNoiserAndOptions,
     GaussianOptionsExampleResultShouldBeCorrect) {
  wfa::measurement::internal::duchy::DifferentialPrivacyParams test_params;
  test_params.set_epsilon(0.35 * std::log(3));
  test_params.set_delta(0.2 / 100000);
  int uncorrupted_party_count = 3;

  auto options = GetFrequencyNoiserAndOptions(
                     test_params, uncorrupted_party_count,
                     LiquidLegionsV2NoiseConfig::DISCRETE_GAUSSIAN)
                     .options;

  EXPECT_EQ(options->contributor_count, uncorrupted_party_count);
  EXPECT_EQ(options->shift_offset, 45);
  EXPECT_EQ(options->truncate_threshold, 45);
  auto gaussianOptions =
      static_cast<math::DistributedDiscreteGaussianNoiseComponentOptions*>(
          options.get());
  EXPECT_NEAR(gaussianOptions->sigma_distributed, 7.956, 0.001);
}

}  // namespace
}  // namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2
