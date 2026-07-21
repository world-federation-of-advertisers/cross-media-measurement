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

using ::wfa::IsOk;
using ::wfa::StatusIs;

TEST(ConditionalMergeImplTest, TestNoNodes) {
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        conditional_merge { pass_through_non_matches: false }
      )pb",
      &config));
  EXPECT_THAT(AttributesUpdaterInterface::Build(config).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(ConditionalMergeImplTest, TestNoCondition) {
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        conditional_merge {
          nodes { update { person_country_code: "UPDATED_COUNTRY_1" } }
          pass_through_non_matches: false
        }
      )pb",
      &config));
  EXPECT_THAT(AttributesUpdaterInterface::Build(config).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(ConditionalMergeImplTest, TestInvalidCondition) {
  // Name and value must be set for EQUAL field filter. So the build of the
  // field filter condition of the 1st node will fail, thus the buid of the
  // conditional merge will fail.
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        conditional_merge {
          nodes {
            condition { op: EQUAL }
            update { person_country_code: "UPDATED_COUNTRY_1" }
          }
          pass_through_non_matches: false
        }
      )pb",
      &config));
  EXPECT_THAT(AttributesUpdaterInterface::Build(config).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(ConditionalMergeImplTest, TestNoUpdate) {
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        conditional_merge {
          nodes {
            condition {
              op: EQUAL
              name: "person_country_code"
              value: "COUNTRY_1"
            }
          }
          pass_through_non_matches: false
        }
      )pb",
      &config));
  EXPECT_THAT(AttributesUpdaterInterface::Build(config).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(ConditionalMergeImplTest, TestUpdateEvents) {
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        conditional_merge {
          nodes {
            condition {
              op: EQUAL
              name: "person_country_code"
              value: "COUNTRY_1"
            }
            update { person_country_code: "UPDATED_COUNTRY_1" }
          }
          nodes {
            condition {
              op: EQUAL
              name: "person_country_code"
              value: "COUNTRY_2"
            }
            update { person_country_code: "UPDATED_COUNTRY_2" }
          }
          pass_through_non_matches: false
        }
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<AttributesUpdaterInterface> updater,
                       AttributesUpdaterInterface::Build(config));

  // Matches the 1st node, update the event.
  LabelerEvent event_1;
  event_1.set_person_country_code("COUNTRY_1");
  EXPECT_THAT(updater->Update(event_1), IsOk());
  EXPECT_EQ(event_1.person_country_code(), "UPDATED_COUNTRY_1");

  // Matches the 2nd node, update the event.
  LabelerEvent event_2;
  event_2.set_person_country_code("COUNTRY_2");
  EXPECT_THAT(updater->Update(event_2), IsOk());
  EXPECT_EQ(event_2.person_country_code(), "UPDATED_COUNTRY_2");
}

TEST(ConditionalMergeImplTest, TestNoMatchingNotPass) {
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        conditional_merge {
          nodes {
            condition {
              op: EQUAL
              name: "person_country_code"
              value: "COUNTRY_1"
            }
            update { person_country_code: "UPDATED_COUNTRY_1" }
          }
          nodes {
            condition {
              op: EQUAL
              name: "person_country_code"
              value: "COUNTRY_2"
            }
            update { person_country_code: "UPDATED_COUNTRY_2" }
          }
          pass_through_non_matches: false
        }
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<AttributesUpdaterInterface> updater,
                       AttributesUpdaterInterface::Build(config));

  // No matching. Returns error status as pass_through_non_matches is false.
  LabelerEvent event;
  event.set_person_country_code("COUNTRY_3");
  EXPECT_THAT(updater->Update(event),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
  EXPECT_EQ(event.person_country_code(), "COUNTRY_3");
}

TEST(ConditionalMergeImplTest, TestNoMatchingNotPassByDefault) {
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        conditional_merge {
          nodes {
            condition {
              op: EQUAL
              name: "person_country_code"
              value: "COUNTRY_1"
            }
            update { person_country_code: "UPDATED_COUNTRY_1" }
          }
          nodes {
            condition {
              op: EQUAL
              name: "person_country_code"
              value: "COUNTRY_2"
            }
            update { person_country_code: "UPDATED_COUNTRY_2" }
          }
        }
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<AttributesUpdaterInterface> updater,
                       AttributesUpdaterInterface::Build(config));

  // No matching. pass_through_non_matches is false by default.
  LabelerEvent event;
  event.set_person_country_code("COUNTRY_3");
  EXPECT_THAT(updater->Update(event),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
  EXPECT_EQ(event.person_country_code(), "COUNTRY_3");
}

TEST(ConditionalMergeImplTest, TestNoMatchingPass) {
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        conditional_merge {
          nodes {
            condition {
              op: EQUAL
              name: "person_country_code"
              value: "COUNTRY_1"
            }
            update { person_country_code: "UPDATED_COUNTRY_1" }
          }
          nodes {
            condition {
              op: EQUAL
              name: "person_country_code"
              value: "COUNTRY_2"
            }
            update { person_country_code: "UPDATED_COUNTRY_2" }
          }
          pass_through_non_matches: true
        }
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<AttributesUpdaterInterface> updater,
                       AttributesUpdaterInterface::Build(config));

  // No matching. Returns OK status as pass_through_non_matches is true.
  LabelerEvent event;
  event.set_person_country_code("COUNTRY_3");
  EXPECT_THAT(updater->Update(event), IsOk());
  EXPECT_EQ(event.person_country_code(), "COUNTRY_3");
}

}  // namespace
}  // namespace wfa_virtual_people
