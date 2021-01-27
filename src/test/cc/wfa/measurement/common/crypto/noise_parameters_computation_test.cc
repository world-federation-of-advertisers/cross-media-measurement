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

#include "gtest/gtest.h"

namespace wfa::measurement::common::crypto {
namespace {

TEST(GetBlindHistogramNoiseOptions, ExampleResultShouldBeCorrect) {
  DifferentialPrivacyParams test_params;
  test_params.set_epsilon(std::log(3) / 30);
  test_params.set_delta(0.2 / 100000);
  int publisher_count = 3;
  int uncorrupted_party_count = 2;

  auto options = GetBlindHistogramNoiseOptions(test_params, publisher_count,
                                               uncorrupted_party_count);

  EXPECT_EQ(options.num, uncorrupted_party_count);
  EXPECT_NEAR(options.p, 0.988, 0.001);
  EXPECT_EQ(options.shift_offset, 1697);
  EXPECT_EQ(options.truncate_threshold, 1697);
}

TEST(GetNoiseForPublisherNoiseOptions, ExampleResultShouldBeCorrect) {
  DifferentialPrivacyParams test_params;
  test_params.set_epsilon(std::log(3) / 30);
  test_params.set_delta(0.2 / 100000);
  int publisher_count = 3;
  int uncorrupted_party_count = 2;

  auto options = GetNoiseForPublisherNoiseOptions(test_params, publisher_count,
                                                  uncorrupted_party_count);

  EXPECT_EQ(options.num, uncorrupted_party_count);
  EXPECT_NEAR(options.p, 0.988, 0.001);
  EXPECT_EQ(options.shift_offset, 1550);
  EXPECT_EQ(options.truncate_threshold, 1550);
}

TEST(GetGlobalReachDpNoiseOptions, ExampleResultShouldBeCorrect) {
  DifferentialPrivacyParams test_params;
  test_params.set_epsilon(0.45 * std::log(3));
  test_params.set_delta(0.2 / 100000);
  int uncorrupted_party_count = 2;

  auto options =
      GetGlobalReachDpNoiseOptions(test_params, uncorrupted_party_count);

  EXPECT_EQ(options.num, uncorrupted_party_count);
  EXPECT_NEAR(options.p, 0.61, 0.001);
  EXPECT_EQ(options.shift_offset, 32);
  EXPECT_EQ(options.truncate_threshold, 32);
}

TEST(GetFrequencyNoiseOptions, ExampleResultShouldBeCorrect) {
  DifferentialPrivacyParams test_params;
  test_params.set_epsilon(0.45 * std::log(3));
  test_params.set_delta(0.2 / 100000);
  int max_frequency = 5;
  int uncorrupted_party_count = 2;

  auto options = GetFrequencyNoiseOptions(test_params, max_frequency,
                                          uncorrupted_party_count);

  EXPECT_EQ(options.num, uncorrupted_party_count);
  EXPECT_NEAR(options.p, 0.781, 0.001);
  EXPECT_EQ(options.shift_offset, 75);
  EXPECT_EQ(options.truncate_threshold, 75);
}

}  // namespace
}  // namespace wfa::measurement::common::crypto