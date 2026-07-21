// Copyright 2022 The Cross-Media Measurement Authors
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

#include "wfa/virtual_people/core/model/multiplicity_impl.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "common_cpp/testing/status_macros.h"
#include "common_cpp/testing/status_matchers.h"
#include "gmock/gmock.h"
#include "google/protobuf/text_format.h"
#include "gtest/gtest.h"
#include "wfa/virtual_people/common/model.pb.h"

namespace wfa_virtual_people {
namespace {

using ::testing::AnyOf;
using ::testing::DoubleNear;
using ::wfa::StatusIs;

constexpr int kEventCount = 10000;

TEST(MultiplicityImplTest, TestMultiplicityRefNotSet) {
  Multiplicity config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        max_value: 1.2
        cap_at_max: true
        person_index_field: "multiplicity_person_index"
        random_seed: "test multiplicity"
      )pb",
      &config));
  EXPECT_THAT(MultiplicityImpl::Build(config).status(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "must set multiplicity_ref"));
}

TEST(MultiplicityImplTest, TestInvalidMultiplicityFieldName) {
  Multiplicity config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        expected_multiplicity_field: "bad field name"
        max_value: 1.2
        cap_at_max: true
        person_index_field: "multiplicity_person_index"
        random_seed: "test multiplicity"
      )pb",
      &config));
  EXPECT_THAT(
      MultiplicityImpl::Build(config).status(),
      StatusIs(absl::StatusCode::kInvalidArgument, "field name is invalid"));
}

TEST(MultiplicityImplTest, TestInvalidMultiplicityFieldType) {
  // expected_multiplicity_field should use a numeric field.
  Multiplicity config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        expected_multiplicity_field: "corrected_demo"
        max_value: 1.2
        cap_at_max: true
        person_index_field: "multiplicity_person_index"
        random_seed: "test multiplicity"
      )pb",
      &config));
  EXPECT_THAT(MultiplicityImpl::Build(config).status(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "field type for multiplicity"));
}

TEST(MultiplicityImplTest, TestPersonIndexFieldNotSet) {
  Multiplicity config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        expected_multiplicity_field: "expected_multiplicity"
        max_value: 1.2
        cap_at_max: true
        random_seed: "test multiplicity"
      )pb",
      &config));
  EXPECT_THAT(MultiplicityImpl::Build(config).status(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "must set person_index_field"));
}

TEST(MultiplicityImplTest, TestInvalidPersonIndexFieldName) {
  Multiplicity config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        expected_multiplicity_field: "expected_multiplicity"
        max_value: 1.2
        cap_at_max: true
        person_index_field: "bad field name"
        random_seed: "test multiplicity"
      )pb",
      &config));
  EXPECT_THAT(
      MultiplicityImpl::Build(config).status(),
      StatusIs(absl::StatusCode::kInvalidArgument, "field name is invalid"));
}

TEST(MultiplicityImplTest, TestInvalidPersonIndexFieldType) {
  // person_index_field should use an integer field.
  Multiplicity config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        expected_multiplicity_field: "expected_multiplicity"
        max_value: 1.2
        cap_at_max: true
        person_index_field: "expected_multiplicity"
        random_seed: "test multiplicity"
      )pb",
      &config));
  EXPECT_THAT(MultiplicityImpl::Build(config).status(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "type for person_index_field"));
}

TEST(MultiplicityImplTest, TestMaxValueNotSet) {
  Multiplicity config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        expected_multiplicity_field: "expected_multiplicity"
        cap_at_max: true
        person_index_field: "multiplicity_person_index"
        random_seed: "test multiplicity"
      )pb",
      &config));
  EXPECT_THAT(
      MultiplicityImpl::Build(config).status(),
      StatusIs(absl::StatusCode::kInvalidArgument, "must set max_value"));
}

TEST(MultiplicityImplTest, TestCapAtMaxNotSet) {
  Multiplicity config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        expected_multiplicity_field: "expected_multiplicity"
        max_value: 1.2
        person_index_field: "multiplicity_person_index"
        random_seed: "test multiplicity"
      )pb",
      &config));
  EXPECT_THAT(
      MultiplicityImpl::Build(config).status(),
      StatusIs(absl::StatusCode::kInvalidArgument, "must set cap_at_max"));
}

TEST(MultiplicityImplTest, TestRandomSeedNotSet) {
  Multiplicity config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        expected_multiplicity_field: "expected_multiplicity"
        cap_at_max: true
        max_value: 1.2
        person_index_field: "multiplicity_person_index"
      )pb",
      &config));
  EXPECT_THAT(
      MultiplicityImpl::Build(config).status(),
      StatusIs(absl::StatusCode::kInvalidArgument, "must set random_seed"));
}

TEST(MultiplicityImplTest, TestInvalidExtractor) {
  MultiplicityExtractor extractor;
  std::vector<const google::protobuf::FieldDescriptor*> person_index_field;
  std::string random_seed = "test seed";
  std::unique_ptr<MultiplicityImpl> multiplicity =
      absl::make_unique<MultiplicityImpl>(
          extractor, MultiplicityImpl::CapMultiplicityAtMax::kYes,
          /*max_value=*/1.5, std::move(person_index_field), random_seed);

  LabelerEvent input;
  input.set_acting_fingerprint(0);
  EXPECT_THAT(multiplicity->ComputeEventMultiplicity(input).status(),
              StatusIs(absl::StatusCode::kInternal, "multiplicity extractor"));
}

TEST(MultiplicityImplTest, TestExtractorNullFunction) {
  MultiplicityFromField from_field;
  MultiplicityExtractor extractor = std::move(from_field);
  std::vector<const google::protobuf::FieldDescriptor*> person_index_field;
  std::string random_seed = "test seed";
  std::unique_ptr<MultiplicityImpl> multiplicity =
      absl::make_unique<MultiplicityImpl>(
          extractor, MultiplicityImpl::CapMultiplicityAtMax::kYes,
          /*max_value=*/1.5, std::move(person_index_field), random_seed);

  LabelerEvent input;
  input.set_acting_fingerprint(0);
  EXPECT_THAT(multiplicity->ComputeEventMultiplicity(input).status(),
              StatusIs(absl::StatusCode::kInternal, "NULL get_value_function"));
}

TEST(MultiplicityImplTest, TestExtractorEmptyField) {
  MultiplicityFromField from_field;
  from_field.get_value_function =
      [](const LabelerEvent& event,
         const std::vector<const google::protobuf::FieldDescriptor*>& source) {
        return 1;
      };
  MultiplicityExtractor extractor = std::move(from_field);
  std::vector<const google::protobuf::FieldDescriptor*> person_index_field;
  std::string random_seed = "test seed";
  std::unique_ptr<MultiplicityImpl> multiplicity =
      absl::make_unique<MultiplicityImpl>(
          extractor, MultiplicityImpl::CapMultiplicityAtMax::kYes,
          /*max_value=*/1.5, std::move(person_index_field), random_seed);

  LabelerEvent input;
  input.set_acting_fingerprint(0);
  EXPECT_THAT(
      multiplicity->ComputeEventMultiplicity(input).status(),
      StatusIs(absl::StatusCode::kInternal, "invalid field_descriptor"));
}

TEST(MultiplicityImplTest, TestExplicitMultiplicityAndCapAtMaxTrue) {
  // Should cap to max_value when cap_at_max = true.
  Multiplicity config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        expected_multiplicity: 2.5
        max_value: 1.3
        cap_at_max: true
        person_index_field: "multiplicity_person_index"
        random_seed: "test multiplicity"
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<MultiplicityImpl> multiplicity,
                       MultiplicityImpl::Build(config));

  int64_t person_total = 0;
  for (int fingerprint = 0; fingerprint < kEventCount; ++fingerprint) {
    LabelerEvent input;
    input.set_acting_fingerprint(fingerprint);
    ASSERT_OK_AND_ASSIGN(int clone_count,
                         multiplicity->ComputeEventMultiplicity(input));
    EXPECT_THAT(clone_count, AnyOf(1, 2));
    person_total += clone_count;
  }

  // Compare to the exact result to make sure C++ and Kotlin implementations
  // behave the same. The result should be around 1.3 * kEventCount = 13000
  EXPECT_EQ(12947, person_total);
}

TEST(MultiplicityImplTest, TestExplicitMultiplicityAndCapAtMaxFalse) {
  // Should return error if > max_value when cap_at_max = false.
  Multiplicity config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        expected_multiplicity: 2.5
        max_value: 1.3
        cap_at_max: false
        person_index_field: "multiplicity_person_index"
        random_seed: "test multiplicity"
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<MultiplicityImpl> multiplicity,
                       MultiplicityImpl::Build(config));

  for (int fingerprint = 0; fingerprint < kEventCount; ++fingerprint) {
    LabelerEvent input;
    input.set_acting_fingerprint(fingerprint);
    EXPECT_THAT(multiplicity->ComputeEventMultiplicity(input).status(),
                StatusIs(absl::StatusCode::kOutOfRange,
                         "exceeds the specified max value"));
  }
}

TEST(MultiplicityImplTest, TestExplicitMultiplicityIsNegative) {
  // Should return error if multiplicity < 0.
  Multiplicity config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        expected_multiplicity: -1.2
        max_value: 1.3
        cap_at_max: false
        person_index_field: "multiplicity_person_index"
        random_seed: "test multiplicity"
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<MultiplicityImpl> multiplicity,
                       MultiplicityImpl::Build(config));

  for (int fingerprint = 0; fingerprint < kEventCount; ++fingerprint) {
    LabelerEvent input;
    input.set_acting_fingerprint(fingerprint);
    EXPECT_THAT(
        multiplicity->ComputeEventMultiplicity(input).status(),
        StatusIs(absl::StatusCode::kOutOfRange, "multiplicity must >= 0"));
  }
}

TEST(MultiplicityImplTest, TestMultiplicityFieldAndCapAtMaxTrue) {
  // Should cap to max_value when cap_at_max = true.
  Multiplicity config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        expected_multiplicity_field: "expected_multiplicity"
        max_value: 1.3
        cap_at_max: true
        person_index_field: "multiplicity_person_index"
        random_seed: "test multiplicity"
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<MultiplicityImpl> multiplicity,
                       MultiplicityImpl::Build(config));

  // multiplicity field is not set, return error
  for (int fingerprint = 0; fingerprint < kEventCount; ++fingerprint) {
    LabelerEvent input;
    input.set_acting_fingerprint(fingerprint);
    EXPECT_THAT(multiplicity->ComputeEventMultiplicity(input).status(),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "multiplicity field is not set"));
  }

  // multiplicity > max_value, cap at max_value
  int64_t person_total = 0;
  for (int fingerprint = 0; fingerprint < kEventCount; ++fingerprint) {
    LabelerEvent input;
    input.set_acting_fingerprint(fingerprint);
    input.set_expected_multiplicity(2);
    ASSERT_OK_AND_ASSIGN(int clone_count,
                         multiplicity->ComputeEventMultiplicity(input));
    EXPECT_THAT(clone_count, AnyOf(1, 2));
    person_total += clone_count;
  }
  // Compare to the exact result to make sure C++ and Kotlin implementations
  // behave the same. The result should be around 1.3 * kEventCount = 13000
  EXPECT_EQ(12947, person_total);

  // multiplicity < max_value
  person_total = 0;
  for (int fingerprint = 0; fingerprint < kEventCount; ++fingerprint) {
    LabelerEvent input;
    input.set_acting_fingerprint(fingerprint);
    input.set_expected_multiplicity(1.2);
    ASSERT_OK_AND_ASSIGN(int clone_count,
                         multiplicity->ComputeEventMultiplicity(input));
    EXPECT_THAT(clone_count, AnyOf(1, 2));
    person_total += clone_count;
  }
  // Compare to the exact result to make sure C++ and Kotlin implementations
  // behave the same. The result should be around 1.2 * kEventCount = 12000
  EXPECT_EQ(11949, person_total);
}

TEST(MultiplicityImplTest, TestMultiplicityFieldAndCapAtMaxFalse) {
  // Should return error if > max_value when cap_at_max = false.
  Multiplicity config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        expected_multiplicity_field: "expected_multiplicity"
        max_value: 1.3
        cap_at_max: false
        person_index_field: "multiplicity_person_index"
        random_seed: "test multiplicity"
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<MultiplicityImpl> multiplicity,
                       MultiplicityImpl::Build(config));

  // multiplicity field is not set, return error
  for (int fingerprint = 0; fingerprint < kEventCount; ++fingerprint) {
    LabelerEvent input;
    input.set_acting_fingerprint(fingerprint);
    EXPECT_THAT(multiplicity->ComputeEventMultiplicity(input).status(),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "multiplicity field is not set"));
  }

  // multiplicity > max_value, return error.
  for (int fingerprint = 0; fingerprint < kEventCount; ++fingerprint) {
    LabelerEvent input;
    input.set_acting_fingerprint(fingerprint);
    input.set_expected_multiplicity(2);
    EXPECT_THAT(multiplicity->ComputeEventMultiplicity(input).status(),
                StatusIs(absl::StatusCode::kOutOfRange,
                         "exceeds the specified max value"));
  }

  // multiplicity < max_value
  int64_t person_total = 0;
  for (int fingerprint = 0; fingerprint < kEventCount; ++fingerprint) {
    LabelerEvent input;
    input.set_acting_fingerprint(fingerprint);
    input.set_expected_multiplicity(1.2);
    ASSERT_OK_AND_ASSIGN(int clone_count,
                         multiplicity->ComputeEventMultiplicity(input));
    EXPECT_THAT(clone_count, AnyOf(1, 2));
    person_total += clone_count;
  }
  // Compare to the exact result to make sure C++ and Kotlin implementations
  // behave the same. The result should be around 1.2 * kEventCount = 12000
  EXPECT_EQ(11949, person_total);
}

TEST(MultiplicityImplTest, TestMultiplicityFieldIsNegative) {
  Multiplicity config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        expected_multiplicity_field: "expected_multiplicity"
        max_value: 1.3
        cap_at_max: false
        person_index_field: "multiplicity_person_index"
        random_seed: "test multiplicity"
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<MultiplicityImpl> multiplicity,
                       MultiplicityImpl::Build(config));

  // Should return error if multiplicity < 0.
  for (int fingerprint = 0; fingerprint < kEventCount; ++fingerprint) {
    LabelerEvent input;
    input.set_acting_fingerprint(fingerprint);
    input.set_expected_multiplicity(-1.2);
    EXPECT_THAT(
        multiplicity->ComputeEventMultiplicity(input).status(),
        StatusIs(absl::StatusCode::kOutOfRange, "multiplicity must >= 0"));
  }
}

TEST(MultiplicityImplTest, TestMultiplicityLessThanOne) {
  Multiplicity config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        expected_multiplicity: 0.3
        max_value: 1.3
        cap_at_max: true
        person_index_field: "multiplicity_person_index"
        random_seed: "test multiplicity"
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<MultiplicityImpl> multiplicity,
                       MultiplicityImpl::Build(config));

  int64_t person_total = 0;
  for (int fingerprint = 0; fingerprint < kEventCount; ++fingerprint) {
    LabelerEvent input;
    input.set_acting_fingerprint(fingerprint);
    ASSERT_OK_AND_ASSIGN(int clone_count,
                         multiplicity->ComputeEventMultiplicity(input));
    EXPECT_THAT(clone_count, AnyOf(0, 1));
    person_total += clone_count;
  }

  // Compare to the exact result to make sure C++ and Kotlin implementations
  // behave the same. The result should be around 0.3 * kEventCount = 3000
  EXPECT_EQ(2947, person_total);
}

}  // namespace
}  // namespace wfa_virtual_people
