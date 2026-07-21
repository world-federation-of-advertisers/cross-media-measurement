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
#include "common_cpp/testing/common_matchers.h"
#include "common_cpp/testing/status_macros.h"
#include "common_cpp/testing/status_matchers.h"
#include "gmock/gmock.h"
#include "google/protobuf/text_format.h"
#include "gtest/gtest.h"
#include "wfa/virtual_people/common/demographic.pb.h"
#include "wfa/virtual_people/common/model.pb.h"
#include "wfa/virtual_people/core/model/attributes_updater.h"

namespace wfa_virtual_people {
namespace {

using ::wfa::EqualsProto;
using ::wfa::IsOk;
using ::wfa::StatusIs;

TEST(ConditionalAssignmentImplTest, TestNoCondition) {
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        conditional_assignment {
          assignments {
            source_field: "acting_demo.gender"
            target_field: "corrected_demo.gender"
          }
        }
      )pb",
      &config));
  EXPECT_THAT(AttributesUpdaterInterface::Build(config).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(ConditionalAssignmentImplTest, TestInvalidCondition) {
  // Name and value must be set for EQUAL field filter. So the build of the
  // field filter condition will fail, thus the buid of the conditional
  // assignment will fail.
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        conditional_assignment {
          condition { op: EQUAL }
          assignments {
            source_field: "acting_demo.gender"
            target_field: "corrected_demo.gender"
          }
        }
      )pb",
      &config));
  EXPECT_THAT(AttributesUpdaterInterface::Build(config).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(ConditionalAssignmentImplTest, TestEmptyAssignments) {
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        conditional_assignment {
          condition { op: HAS name: "acting_demo.gender" }
        }
      )pb",
      &config));
  EXPECT_THAT(AttributesUpdaterInterface::Build(config).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(ConditionalAssignmentImplTest, TestNoSourceField) {
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        conditional_assignment {
          condition { op: HAS name: "acting_demo.gender" }
          assignments { target_field: "corrected_demo.gender" }
        }
      )pb",
      &config));
  EXPECT_THAT(AttributesUpdaterInterface::Build(config).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(ConditionalAssignmentImplTest, TestNoTargetField) {
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        conditional_assignment {
          condition { op: HAS name: "acting_demo.gender" }
          assignments { source_field: "acting_demo.gender" }
        }
      )pb",
      &config));
  EXPECT_THAT(AttributesUpdaterInterface::Build(config).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(ConditionalAssignmentImplTest, TestSingleAssignment) {
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        conditional_assignment {
          condition { op: HAS name: "acting_demo.gender" }
          assignments {
            source_field: "acting_demo.gender"
            target_field: "corrected_demo.gender"
          }
        }
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<AttributesUpdaterInterface> updater,
                       AttributesUpdaterInterface::Build(config));

  // Matches the condition. Updates the event.
  LabelerEvent event_1;
  event_1.mutable_acting_demo()->set_gender(GENDER_FEMALE);
  EXPECT_THAT(updater->Update(event_1), IsOk());
  LabelerEvent expected_event_1;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        acting_demo { gender: GENDER_FEMALE }
        corrected_demo { gender: GENDER_FEMALE }
      )pb",
      &expected_event_1));
  EXPECT_THAT(event_1, EqualsProto(expected_event_1));

  // Does not match the condition. Does nothing
  LabelerEvent event_2;
  EXPECT_THAT(updater->Update(event_2), IsOk());
  EXPECT_THAT(event_2, EqualsProto(LabelerEvent()));
}

TEST(ConditionalAssignmentImplTest, TestMultipleAssignments) {
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        conditional_assignment {
          condition {
            op: AND
            sub_filters { op: HAS name: "acting_demo.gender" }
            sub_filters { op: HAS name: "acting_demo.age.min_age" }
            sub_filters { op: HAS name: "acting_demo.age.max_age" }
          }
          assignments {
            source_field: "acting_demo.gender"
            target_field: "corrected_demo.gender"
          }
          assignments {
            source_field: "acting_demo.age.min_age"
            target_field: "corrected_demo.age.min_age"
          }
          assignments {
            source_field: "acting_demo.age.max_age"
            target_field: "corrected_demo.age.max_age"
          }
        }
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<AttributesUpdaterInterface> updater,
                       AttributesUpdaterInterface::Build(config));

  // Matches the condition. Updates the event.
  LabelerEvent event_1;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        acting_demo {
          gender: GENDER_FEMALE
          age { min_age: 25 max_age: 29 }
        }
      )pb",
      &event_1));
  EXPECT_THAT(updater->Update(event_1), IsOk());
  LabelerEvent expected_event_1;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        acting_demo {
          gender: GENDER_FEMALE
          age { min_age: 25 max_age: 29 }
        }
        corrected_demo {
          gender: GENDER_FEMALE
          age { min_age: 25 max_age: 29 }
        }
      )pb",
      &expected_event_1));
  EXPECT_THAT(event_1, EqualsProto(expected_event_1));

  // Does not match the condition as acting_demo.gender is not set. Does
  // nothing.
  LabelerEvent event_2;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        acting_demo { age { min_age: 25 max_age: 29 } }
      )pb",
      &event_2));
  EXPECT_THAT(updater->Update(event_2), IsOk());
  LabelerEvent expected_event_2;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        acting_demo { age { min_age: 25 max_age: 29 } }
      )pb",
      &expected_event_2));
  EXPECT_THAT(event_2, EqualsProto(expected_event_2));

  // Does not match the condition as acting_demo.age.max_age is not set. Does
  // nothing.
  LabelerEvent event_3;
  ASSERT_TRUE(
      google::protobuf::TextFormat::ParseFromString(R"pb(
                                                      acting_demo {
                                                        gender: GENDER_FEMALE
                                                        age { min_age: 25 }
                                                      }
                                                    )pb",
                                                    &event_3));
  EXPECT_THAT(updater->Update(event_3), IsOk());
  LabelerEvent expected_event_3;
  ASSERT_TRUE(
      google::protobuf::TextFormat::ParseFromString(R"pb(
                                                      acting_demo {
                                                        gender: GENDER_FEMALE
                                                        age { min_age: 25 }
                                                      }
                                                    )pb",
                                                    &expected_event_3));
  EXPECT_THAT(event_3, EqualsProto(expected_event_3));

  // Does not match the condition as acting_demo.age.min_age is not set. Does
  // nothing.
  LabelerEvent event_4;
  ASSERT_TRUE(
      google::protobuf::TextFormat::ParseFromString(R"pb(
                                                      acting_demo {
                                                        gender: GENDER_FEMALE
                                                        age { max_age: 29 }
                                                      }
                                                    )pb",
                                                    &event_4));
  EXPECT_THAT(updater->Update(event_4), IsOk());
  LabelerEvent expected_event_4;
  ASSERT_TRUE(
      google::protobuf::TextFormat::ParseFromString(R"pb(
                                                      acting_demo {
                                                        gender: GENDER_FEMALE
                                                        age { max_age: 29 }
                                                      }
                                                    )pb",
                                                    &expected_event_4));
  EXPECT_THAT(event_4, EqualsProto(expected_event_4));
}

TEST(ConditionalAssignmentImplTest, SkipAssignmentsForUnsetField) {
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        conditional_assignment {
          condition { op: TRUE }
          assignments {
            source_field: "acting_demo.gender"
            target_field: "corrected_demo.gender"
          }
        }
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<AttributesUpdaterInterface> updater,
                       AttributesUpdaterInterface::Build(config));

  // Skip the conditional assignment as the source field is not set.
  LabelerEvent event;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        corrected_demo { gender: GENDER_FEMALE }
      )pb",
      &event));
  EXPECT_THAT(updater->Update(event), IsOk());
  LabelerEvent expected_event;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        corrected_demo { gender: GENDER_FEMALE }
      )pb",
      &expected_event));
  EXPECT_THAT(event, EqualsProto(expected_event));
}

}  // namespace
}  // namespace wfa_virtual_people
