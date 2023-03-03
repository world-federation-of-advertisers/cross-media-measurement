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

namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2 {
namespace {

TEST(GetBlindHistogramNoiseOptions, ExampleResultShouldBeCorrect) {
  wfa::measurement::internal::duchy::DifferentialPrivacyParams test_params;
  test_params.set_epsilon(std::log(3) / 10);
  test_params.set_delta(0.2 / 100000);
  int uncorrupted_party_count = 2;

  auto options = GetBlindHistogramGeometricNoiseOptions(
      test_params, uncorrupted_party_count);

  EXPECT_EQ(options.contributor_count, uncorrupted_party_count);
  EXPECT_NEAR(options.p, 0.947, 0.001);
  EXPECT_EQ(options.shift_offset, 291);
  EXPECT_EQ(options.truncate_threshold, 291);
}

TEST(GetNoiseForPublisherNoiseOptions, ExampleResultShouldBeCorrect) {
  wfa::measurement::internal::duchy::DifferentialPrivacyParams test_params;
  test_params.set_epsilon(std::log(3) / 10);
  test_params.set_delta(0.2 / 100000);
  int publisher_count = 3;
  int uncorrupted_party_count = 2;

  auto options = GetNoiseForPublisherGeometricNoiseOptions(
      test_params, publisher_count, uncorrupted_party_count);

  EXPECT_EQ(options.contributor_count, uncorrupted_party_count);
  EXPECT_NEAR(options.p, 0.964, 0.001);
  EXPECT_EQ(options.shift_offset, 447);
  EXPECT_EQ(options.truncate_threshold, 447);
}

TEST(GetGlobalReachDpNoiseOptions, ExampleResultShouldBeCorrect) {
  wfa::measurement::internal::duchy::DifferentialPrivacyParams test_params;
  test_params.set_epsilon(0.35 * std::log(3));
  test_params.set_delta(0.2 / 100000);
  int uncorrupted_party_count = 2;

  auto options = GetGlobalReachDpGeometricNoiseOptions(test_params,
                                                       uncorrupted_party_count);

  EXPECT_EQ(options.contributor_count, uncorrupted_party_count);
  EXPECT_NEAR(options.p, 0.681, 0.001);
  EXPECT_EQ(options.shift_offset, 41);
  EXPECT_EQ(options.truncate_threshold, 41);
}

TEST(GetFrequencyNoiseOptions, ExampleResultShouldBeCorrect) {
  wfa::measurement::internal::duchy::DifferentialPrivacyParams test_params;
  test_params.set_epsilon(0.35 * std::log(3));
  test_params.set_delta(0.2 / 100000);
  int uncorrupted_party_count = 2;

  auto options =
      GetFrequencyGeometricNoiseOptions(test_params, uncorrupted_party_count);

  EXPECT_EQ(options.contributor_count, uncorrupted_party_count);
  EXPECT_NEAR(options.p, 0.825, 0.001);
  EXPECT_EQ(options.shift_offset, 84);
  EXPECT_EQ(options.truncate_threshold, 84);
}

}  // namespace
}  // namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2
