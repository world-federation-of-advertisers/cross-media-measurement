// Copyright 2023 The Cross-Media Measurement Authors
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

using ::testing::DoubleNear;
using ::wfa::EqualsProto;
using ::wfa::IsOk;
using ::wfa::StatusIs;

// randomness_field value is from 1 to kRandomnessKeyNumber.
constexpr int kRandomnessKeyNumber = 10000;
// target_field value is from 0 to kTargetKeyNumber.
constexpr int kTargetKeyNumber = 100;

TEST(GeometricShredderImplTest, TestNegativePsi) {
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        geometric_shredder {
          psi: -0.01
          randomness_field: "labeler_input.event_id.id_fingerprint"
          target_field: "acting_fingerprint"
          random_seed: "seed"
        }
      )pb",
      &config));
  EXPECT_THAT(AttributesUpdaterInterface::Build(config).status(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "Psi is not in [0, 1] in GeometricShredder:"));
}

TEST(GeometricShredderImplTest, TestPsiLargerThanOne) {
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        geometric_shredder {
          psi: 1.01
          randomness_field: "labeler_input.event_id.id_fingerprint"
          target_field: "acting_fingerprint"
          random_seed: "seed"
        }
      )pb",
      &config));
  EXPECT_THAT(AttributesUpdaterInterface::Build(config).status(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "Psi is not in [0, 1] in GeometricShredder:"));
}

TEST(GeometricShredderImplTest, TestInvalidRandomnessField) {
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        geometric_shredder {
          psi: 0.5
          randomness_field: "labeler_input.event_id.__INVALID_FIELD__"
          target_field: "acting_fingerprint"
          random_seed: "seed"
        }
      )pb",
      &config));
  EXPECT_THAT(AttributesUpdaterInterface::Build(config).status(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "The field name is invalid:"));
}

TEST(GeometricShredderImplTest, TestNonUint64RandomnessField) {
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        geometric_shredder {
          psi: 0.5
          randomness_field: "labeler_input.event_id.id"
          target_field: "acting_fingerprint"
          random_seed: "seed"
        }
      )pb",
      &config));
  EXPECT_THAT(AttributesUpdaterInterface::Build(config).status(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "randomness_field type is not uint64 in "
                       "GeometricShredder:"));
}

TEST(GeometricShredderImplTest, TestInvalidTargetField) {
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        geometric_shredder {
          psi: 0.5
          randomness_field: "labeler_input.event_id.id_fingerprint"
          target_field: "__INVALID_FIELD__"
          random_seed: "seed"
        }
      )pb",
      &config));
  EXPECT_THAT(AttributesUpdaterInterface::Build(config).status(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "The field name is invalid:"));
}

TEST(GeometricShredderImplTest, TestNonUint64TargetField) {
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        geometric_shredder {
          psi: 0.5
          randomness_field: "labeler_input.event_id.id_fingerprint"
          target_field: "acting_demo_id_space"
          random_seed: "seed"
        }
      )pb",
      &config));
  EXPECT_THAT(
      AttributesUpdaterInterface::Build(config).status(),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "target_field type is not uint64 in GeometricShredder:"));
}

TEST(GeometricShredderImplTest, TestRandomnessFieldNotSet) {
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        geometric_shredder {
          psi: 1
          randomness_field: "labeler_input.event_id.id_fingerprint"
          target_field: "acting_fingerprint"
          random_seed: "seed"
        }
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<AttributesUpdaterInterface> updater,
                       AttributesUpdaterInterface::Build(config));

  LabelerEvent event;
  event.set_acting_fingerprint(1);
  EXPECT_THAT(updater->Update(event),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "The randomness field is not set in the event."));
}

TEST(GeometricShredderImplTest, TestTargetFieldNotSet) {
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        geometric_shredder {
          psi: 1
          randomness_field: "labeler_input.event_id.id_fingerprint"
          target_field: "acting_fingerprint"
          random_seed: "seed"
        }
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<AttributesUpdaterInterface> updater,
                       AttributesUpdaterInterface::Build(config));

  LabelerEvent event;
  event.mutable_labeler_input()->mutable_event_id()->set_id_fingerprint(1);
  EXPECT_THAT(updater->Update(event),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "The target field is not set in the event."));
}

TEST(GeometricShredderImplTest, TestNoShredWithPsiAsZero) {
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        geometric_shredder {
          psi: 0
          randomness_field: "labeler_input.event_id.id_fingerprint"
          target_field: "acting_fingerprint"
          random_seed: "seed"
        }
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<AttributesUpdaterInterface> updater,
                       AttributesUpdaterInterface::Build(config));

  for (uint64_t acting_fp = 0; acting_fp < kTargetKeyNumber; ++acting_fp) {
    for (uint64_t event_id_fp = 1; event_id_fp <= kRandomnessKeyNumber;
         ++event_id_fp) {
      LabelerEvent event;
      event.mutable_labeler_input()->mutable_event_id()->set_id_fingerprint(
          event_id_fp);
      event.set_acting_fingerprint(acting_fp);
      EXPECT_THAT(updater->Update(event), IsOk());
      EXPECT_EQ(event.acting_fingerprint(), acting_fp);
    }
  }
}

TEST(GeometricShredderImplTest, TestAlwaysShredWithPsiAsOne) {
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        geometric_shredder {
          psi: 1
          randomness_field: "labeler_input.event_id.id_fingerprint"
          target_field: "acting_fingerprint"
          random_seed: "seed"
        }
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<AttributesUpdaterInterface> updater,
                       AttributesUpdaterInterface::Build(config));

  for (uint64_t acting_fp = 0; acting_fp < kTargetKeyNumber; ++acting_fp) {
    for (uint64_t event_id_fp = 1; event_id_fp <= kRandomnessKeyNumber;
         ++event_id_fp) {
      LabelerEvent event;
      event.mutable_labeler_input()->mutable_event_id()->set_id_fingerprint(
          event_id_fp);
      event.set_acting_fingerprint(acting_fp);
      EXPECT_THAT(updater->Update(event), IsOk());
      EXPECT_NE(event.acting_fingerprint(), acting_fp);
    }
  }
}

TEST(GeometricShredderImplTest, TestNoShredWithPsiAsOneRandomnessValueAsZero) {
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        geometric_shredder {
          psi: 1
          randomness_field: "labeler_input.event_id.id_fingerprint"
          target_field: "acting_fingerprint"
          random_seed: "seed"
        }
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<AttributesUpdaterInterface> updater,
                       AttributesUpdaterInterface::Build(config));

  for (uint64_t acting_fp = 0; acting_fp < kTargetKeyNumber; ++acting_fp) {
    LabelerEvent event;
    event.mutable_labeler_input()->mutable_event_id()->set_id_fingerprint(0);
    event.set_acting_fingerprint(acting_fp);
    EXPECT_THAT(updater->Update(event), IsOk());
    EXPECT_EQ(event.acting_fingerprint(), acting_fp);
  }
}

TEST(GeometricShredderImplTest, TestShredWithProbability) {
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        geometric_shredder {
          psi: 0.65
          randomness_field: "labeler_input.event_id.id_fingerprint"
          target_field: "acting_fingerprint"
          random_seed: "seed"
        }
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<AttributesUpdaterInterface> updater,
                       AttributesUpdaterInterface::Build(config));

  for (uint64_t acting_fp = 0; acting_fp < kTargetKeyNumber; ++acting_fp) {
    int shred_count = 0;
    for (uint64_t event_id_fp = 1; event_id_fp <= kRandomnessKeyNumber;
         ++event_id_fp) {
      LabelerEvent event;
      event.mutable_labeler_input()->mutable_event_id()->set_id_fingerprint(
          event_id_fp);
      event.set_acting_fingerprint(acting_fp);
      EXPECT_THAT(updater->Update(event), IsOk());
      if (event.acting_fingerprint() != acting_fp) {
        ++shred_count;
      }
    }
    // Compare to the exact result to make sure C++ and Kotlin implementations
    // behave the same. The result should be around psi * kRandomnessKeyNumber =
    // 6500.
    EXPECT_EQ(6533, shred_count);
  }

  // Test again with different value in randomness field(id_fingerprint).
  for (uint64_t acting_fp = 0; acting_fp < kTargetKeyNumber; ++acting_fp) {
    int shred_count = 0;
    for (uint64_t event_id_fp = 1; event_id_fp <= kRandomnessKeyNumber;
         ++event_id_fp) {
      LabelerEvent event;
      event.mutable_labeler_input()->mutable_event_id()->set_id_fingerprint(
          event_id_fp + 20000);
      event.set_acting_fingerprint(acting_fp);
      EXPECT_THAT(updater->Update(event), IsOk());
      if (event.acting_fingerprint() != acting_fp) {
        ++shred_count;
      }
    }
    // Compare to the exact result to make sure C++ and Kotlin implementations
    // behave the same. The result should be around psi * kRandomnessKeyNumber =
    // 6500.
    EXPECT_EQ(6442, shred_count);
  }
}

}  // namespace
}  // namespace wfa_virtual_people
