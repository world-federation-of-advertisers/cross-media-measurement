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

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "common_cpp/testing/status_macros.h"
#include "common_cpp/testing/status_matchers.h"
#include "gmock/gmock.h"
#include "google/protobuf/text_format.h"
#include "gtest/gtest.h"
#include "wfa/virtual_people/common/model.pb.h"
#include "wfa/virtual_people/core/model/attributes_updater.h"

namespace wfa_virtual_people {
namespace {

using ::testing::DoubleNear;
using ::testing::Pair;
using ::testing::UnorderedElementsAre;
using ::wfa::IsOk;
using ::wfa::StatusIs;

constexpr int kSeedNumber = 10000;

TEST(UpdateMatrixImplTest, TestNoRows) {
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        update_matrix {
          columns { person_country_code: "COUNTRY_1" }
          columns { person_country_code: "COUNTRY_2" }
          pass_through_non_matches: false
          random_seed: "TestSeed"
        }
      )pb",
      &config));
  EXPECT_THAT(AttributesUpdaterInterface::Build(config).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(UpdateMatrixImplTest, TestNoColumns) {
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        update_matrix {
          rows { person_country_code: "UPDATED_COUNTRY_1" }
          rows { person_country_code: "UPDATED_COUNTRY_2" }
          pass_through_non_matches: false
          random_seed: "TestSeed"
        }
      )pb",
      &config));
  EXPECT_THAT(AttributesUpdaterInterface::Build(config).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(UpdateMatrixImplTest, TestProbabilitiesCountNotMatch) {
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        update_matrix {
          columns { person_country_code: "COUNTRY_1" }
          columns { person_country_code: "COUNTRY_2" }
          rows { person_country_code: "UPDATED_COUNTRY_1" }
          rows { person_country_code: "UPDATED_COUNTRY_2" }
          probabilities: 0.8
          probabilities: 0.2
          probabilities: 0.2
          probabilities: 0.8
          probabilities: 0.8
          pass_through_non_matches: false
          random_seed: "TestSeed"
        }
      )pb",
      &config));
  EXPECT_THAT(AttributesUpdaterInterface::Build(config).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(UpdateMatrixImplTest, TestInvalidProbability) {
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        update_matrix {
          columns { person_country_code: "COUNTRY_1" }
          columns { person_country_code: "COUNTRY_2" }
          rows { person_country_code: "UPDATED_COUNTRY_1" }
          rows { person_country_code: "UPDATED_COUNTRY_2" }
          probabilities: -1
          probabilities: 0.2
          probabilities: 2
          probabilities: 0.8
          pass_through_non_matches: false
          random_seed: "TestSeed"
        }
      )pb",
      &config));
  EXPECT_THAT(AttributesUpdaterInterface::Build(config).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(UpdateMatrixImplTest, TestOutputDistribution) {
  // Matrix:
  //                     "COUNTRY_1" "COUNTRY_2"
  // "UPDATED_COUNTRY_1"    0.8         0.2
  // "UPDATED_COUNTRY_2"    0.2         0.8
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        update_matrix {
          columns { person_country_code: "COUNTRY_1" }
          columns { person_country_code: "COUNTRY_2" }
          rows { person_country_code: "UPDATED_COUNTRY_1" }
          rows { person_country_code: "UPDATED_COUNTRY_2" }
          probabilities: 0.8
          probabilities: 0.2
          probabilities: 0.2
          probabilities: 0.8
          pass_through_non_matches: false
          random_seed: "TestSeed"
        }
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<AttributesUpdaterInterface> updater,
                       AttributesUpdaterInterface::Build(config));

  // When the input person_country_code is "COUNTRY_1", the output probability
  // distribution is
  // person_country_code   probability
  // "UPDATED_COUNTRY_1"   0.8
  // "UPDATED_COUNTRY_2"   0.2
  absl::flat_hash_map<std::string, int32_t> output_counts_1;
  for (int seed = 0; seed < kSeedNumber; ++seed) {
    LabelerEvent event;
    event.set_person_country_code("COUNTRY_1");
    event.set_acting_fingerprint(seed);
    EXPECT_THAT(updater->Update(event), IsOk());
    ++output_counts_1[event.person_country_code()];
  }
  // Compares to the exact values to make sure the Kotlin and C++ implementation
  // behave the same.
  EXPECT_THAT(output_counts_1,
              UnorderedElementsAre(Pair("UPDATED_COUNTRY_1", 7993),
                                   Pair("UPDATED_COUNTRY_2", 2007)));

  // When the input person_country_code is "COUNTRY_2", the output probability
  // distribution is
  // person_country_code   probability
  // "UPDATED_COUNTRY_1"   0.2
  // "UPDATED_COUNTRY_2"   0.8
  absl::flat_hash_map<std::string, int32_t> output_counts_2;
  for (int seed = 0; seed < kSeedNumber; ++seed) {
    LabelerEvent event;
    event.set_person_country_code("COUNTRY_2");
    event.set_acting_fingerprint(seed);
    EXPECT_THAT(updater->Update(event), IsOk());
    ++output_counts_2[event.person_country_code()];
  }
  // Compares to the exact values to make sure the Kotlin and C++ implementation
  // behave the same.
  EXPECT_THAT(output_counts_2,
              UnorderedElementsAre(Pair("UPDATED_COUNTRY_1", 1944),
                                   Pair("UPDATED_COUNTRY_2", 8056)));
}

TEST(UpdateMatrixImplTest, TestNotNormalized) {
  // Matrix:
  //                     "COUNTRY_1" "COUNTRY_2"
  // "UPDATED_COUNTRY_1"    1.6         0.2
  // "UPDATED_COUNTRY_2"    0.4         0.8
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        update_matrix {
          columns { person_country_code: "COUNTRY_1" }
          columns { person_country_code: "COUNTRY_2" }
          rows { person_country_code: "UPDATED_COUNTRY_1" }
          rows { person_country_code: "UPDATED_COUNTRY_2" }
          probabilities: 1.6
          probabilities: 0.2
          probabilities: 0.4
          probabilities: 0.8
          pass_through_non_matches: false
          random_seed: "TestSeed"
        }
      )pb",
      &config));
  EXPECT_THAT(AttributesUpdaterInterface::Build(config).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(UpdateMatrixImplTest, TestNoMatchingNotPass) {
  // Matrix:
  //                     "COUNTRY_1" "COUNTRY_2"
  // "UPDATED_COUNTRY_1"    0.8         0.2
  // "UPDATED_COUNTRY_2"    0.2         0.8
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        update_matrix {
          columns { person_country_code: "COUNTRY_1" }
          columns { person_country_code: "COUNTRY_2" }
          rows { person_country_code: "UPDATED_COUNTRY_1" }
          rows { person_country_code: "UPDATED_COUNTRY_2" }
          probabilities: 0.8
          probabilities: 0.2
          probabilities: 0.2
          probabilities: 0.8
          pass_through_non_matches: false
          random_seed: "TestSeed"
        }
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<AttributesUpdaterInterface> updater,
                       AttributesUpdaterInterface::Build(config));

  LabelerEvent event;
  event.set_person_country_code("COUNTRY_3");
  event.set_acting_fingerprint(0);
  EXPECT_THAT(updater->Update(event),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
  EXPECT_EQ(event.person_country_code(), "COUNTRY_3");
}

TEST(UpdateMatrixImplTest, TestNoMatchingPass) {
  // Matrix:
  //                     "COUNTRY_1" "COUNTRY_2"
  // "UPDATED_COUNTRY_1"    0.8         0.2
  // "UPDATED_COUNTRY_2"    0.2         0.8
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        update_matrix {
          columns { person_country_code: "COUNTRY_1" }
          columns { person_country_code: "COUNTRY_2" }
          rows { person_country_code: "UPDATED_COUNTRY_1" }
          rows { person_country_code: "UPDATED_COUNTRY_2" }
          probabilities: 0.8
          probabilities: 0.2
          probabilities: 0.2
          probabilities: 0.8
          pass_through_non_matches: true
          random_seed: "TestSeed"
        }
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<AttributesUpdaterInterface> updater,
                       AttributesUpdaterInterface::Build(config));

  LabelerEvent event;
  event.set_person_country_code("COUNTRY_3");
  event.set_acting_fingerprint(0);
  EXPECT_THAT(updater->Update(event), IsOk());
  EXPECT_EQ(event.person_country_code(), "COUNTRY_3");
}

TEST(UpdateMatrixImplTest, TestHashFieldMask) {
  // Matrix:
  //                     "COUNTRY_1"
  // "UPDATED_COUNTRY_1"     1
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        update_matrix {
          columns {
            person_country_code: "COUNTRY_1"
            person_region_code: "REGION_1"
          }
          rows { person_country_code: "UPDATED_COUNTRY_1" }
          probabilities: 1
          hash_field_mask { paths: "person_country_code" }
          pass_through_non_matches: false
          random_seed: "TestSeed"
        }
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<AttributesUpdaterInterface> updater,
                       AttributesUpdaterInterface::Build(config));

  // Only person_country_code is considered based on the settings in
  // hash_field_mask.
  // person_country_code matches.
  LabelerEvent event_1;
  event_1.set_person_country_code("COUNTRY_1");
  event_1.set_person_region_code("REGION_2");
  event_1.set_acting_fingerprint(0);
  EXPECT_THAT(updater->Update(event_1), IsOk());
  EXPECT_EQ(event_1.person_country_code(), "UPDATED_COUNTRY_1");

  // person_country_code matches.
  LabelerEvent event_2;
  event_2.set_person_country_code("COUNTRY_1");
  event_2.set_acting_fingerprint(0);
  EXPECT_THAT(updater->Update(event_2), IsOk());
  EXPECT_EQ(event_2.person_country_code(), "UPDATED_COUNTRY_1");

  // No match.
  LabelerEvent event_3;
  event_3.set_person_country_code("COUNTRY_2");
  event_3.set_person_region_code("REGION_1");
  event_3.set_acting_fingerprint(0);
  EXPECT_THAT(updater->Update(event_3),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
  EXPECT_EQ(event_3.person_country_code(), "COUNTRY_2");
}

TEST(UpdateMatrixImplTest, TestHashFieldMaskFieldNotSet) {
  // Matrix:
  //                     "COUNTRY_1"
  // "UPDATED_COUNTRY_1"     1
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        update_matrix {
          columns { person_region_code: "REGION_1" }
          rows {
            person_country_code: "COUNTRY_NOT_SET"
            person_region_code: "UPDATED_REGION_1"
          }
          probabilities: 1
          hash_field_mask {
            paths: "person_country_code"
            paths: "person_region_code"
          }
          pass_through_non_matches: false
          random_seed: "TestSeed"
        }
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<AttributesUpdaterInterface> updater,
                       AttributesUpdaterInterface::Build(config));

  // Both person_country_code and person_region_code are considered based on the
  // settings in hash_field_mask.
  // person_country_code must be not set, and person_region_code must be
  // "REGION_1" to match.
  LabelerEvent event_1;
  event_1.set_person_region_code("REGION_1");
  event_1.set_acting_fingerprint(0);
  EXPECT_THAT(updater->Update(event_1), IsOk());
  EXPECT_EQ(event_1.person_country_code(), "COUNTRY_NOT_SET");
  EXPECT_EQ(event_1.person_region_code(), "UPDATED_REGION_1");

  // person_country_code is set. No match.
  LabelerEvent event_2;
  event_2.set_person_country_code("COUNTRY_1");
  event_2.set_person_region_code("REGION_1");
  event_2.set_acting_fingerprint(0);
  EXPECT_THAT(updater->Update(event_2),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
  EXPECT_EQ(event_2.person_country_code(), "COUNTRY_1");
  EXPECT_EQ(event_2.person_region_code(), "REGION_1");
}

}  // namespace
}  // namespace wfa_virtual_people
