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

#include "wfa/virtual_people/core/model/utils/field_filters_matcher.h"

#include <memory>
#include <vector>

#include "absl/status/statusor.h"
#include "common_cpp/testing/status_macros.h"
#include "common_cpp/testing/status_matchers.h"
#include "gmock/gmock.h"
#include "google/protobuf/text_format.h"
#include "gtest/gtest.h"
#include "wfa/virtual_people/common/field_filter.pb.h"
#include "wfa/virtual_people/common/model.pb.h"

namespace wfa_virtual_people {
namespace {

using ::wfa::StatusIs;

TEST(FieldFiltersMatcherTest, EmptyConfig) {
  std::vector<const FieldFilterProto*> filter_configs;
  EXPECT_THAT(FieldFiltersMatcher::Build(filter_configs).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(FieldFiltersMatcherTest, InvalidFieldFilterProto) {
  std::vector<const FieldFilterProto*> filter_configs;
  // This is invalid. EQUAL requires name and value.
  FieldFilterProto filter_config;
  filter_config.set_op(FieldFilterProto::EQUAL);
  filter_configs.emplace_back(&filter_config);
  EXPECT_THAT(FieldFiltersMatcher::Build(filter_configs).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(FieldFiltersMatcherTest, TestGetFirstMatch) {
  std::vector<const FieldFilterProto*> filter_configs;
  // Match when person_country_code is "country_code_1".
  FieldFilterProto filter_config_1;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "person_country_code" op: EQUAL value: "country_code_1"
      )pb",
      &filter_config_1));
  filter_configs.emplace_back(&filter_config_1);
  // Match when person_country_code is "country_code_2".
  FieldFilterProto filter_config_2;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "person_country_code" op: EQUAL value: "country_code_2"
      )pb",
      &filter_config_2));
  filter_configs.emplace_back(&filter_config_2);
  // Match when person_country_code is "country_code_3".
  FieldFilterProto filter_config_3;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "person_country_code" op: EQUAL value: "country_code_3"
      )pb",
      &filter_config_3));
  filter_configs.emplace_back(&filter_config_3);
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldFiltersMatcher> matcher,
                       FieldFiltersMatcher::Build(filter_configs));

  // The 2nd filter matches. Returns 1.
  LabelerEvent event_1;
  event_1.set_person_country_code("country_code_2");
  EXPECT_EQ(matcher->GetFirstMatch(event_1), 1);

  // No matches. Returns -1.
  LabelerEvent event_2;
  event_2.set_person_country_code("country_code_4");
  EXPECT_EQ(matcher->GetFirstMatch(event_2), -1);
}

}  // namespace
}  // namespace wfa_virtual_people
