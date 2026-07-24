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

#include "wfa/virtual_people/core/model/utils/hash_field_mask_matcher.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

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

using ::google::protobuf::FieldMask;
using ::wfa::StatusIs;

FieldMask MakeFieldMask(absl::Span<const std::string> paths) {
  FieldMask field_mask;
  for (const std::string& path : paths) {
    field_mask.add_paths(path);
  }
  return field_mask;
}

TEST(HashFieldMaskMatcherTest, EmptyEvents) {
  std::vector<const LabelerEvent*> events;
  FieldMask hash_field_mask = MakeFieldMask({"person_country_code"});
  EXPECT_THAT(HashFieldMaskMatcher::Build(events, hash_field_mask).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(HashFieldMaskMatcherTest, NullEvent) {
  std::vector<const LabelerEvent*> events;
  events.emplace_back(nullptr);
  FieldMask hash_field_mask = MakeFieldMask({"person_country_code"});
  EXPECT_THAT(HashFieldMaskMatcher::Build(events, hash_field_mask).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(HashFieldMaskMatcherTest, EmptyHashFieldMask) {
  std::vector<const LabelerEvent*> events;
  LabelerEvent input_event;
  input_event.set_person_country_code("COUNTRY_1");
  events.emplace_back(&input_event);
  FieldMask hash_field_mask;
  EXPECT_THAT(HashFieldMaskMatcher::Build(events, hash_field_mask).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(HashFieldMaskMatcherTest, EventsHaveSameHash) {
  std::vector<const LabelerEvent*> events;
  LabelerEvent input_event_1;
  input_event_1.set_person_country_code("COUNTRY_1");
  input_event_1.set_person_region_code("REGION_1");
  events.emplace_back(&input_event_1);
  LabelerEvent input_event_2;
  input_event_2.set_person_country_code("COUNTRY_1");
  input_event_2.set_person_region_code("REGION_2");
  events.emplace_back(&input_event_2);

  FieldMask hash_field_mask = MakeFieldMask({"person_country_code"});
  EXPECT_THAT(HashFieldMaskMatcher::Build(events, hash_field_mask).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(HashFieldMaskMatcherTest, TestGetMatch) {
  std::vector<const LabelerEvent*> events;
  LabelerEvent input_event_1;
  input_event_1.set_person_country_code("COUNTRY_1");
  input_event_1.set_person_region_code("REGION_1");
  events.emplace_back(&input_event_1);
  LabelerEvent input_event_2;
  input_event_2.set_person_country_code("COUNTRY_2");
  input_event_2.set_person_region_code("REGION_2");
  events.emplace_back(&input_event_2);

  FieldMask hash_field_mask = MakeFieldMask({"person_country_code"});
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<HashFieldMaskMatcher> matcher,
                       HashFieldMaskMatcher::Build(events, hash_field_mask));

  // events[0] matches. Returns 0.
  LabelerEvent event_1;
  event_1.set_person_country_code("COUNTRY_1");
  EXPECT_EQ(matcher->GetMatch(event_1), 0);

  // events[0] matches. Returns 0.
  // person_region_code is ignored as not included in hash_field_mask.
  LabelerEvent event_2;
  event_2.set_person_country_code("COUNTRY_1");
  event_2.set_person_region_code("REGION_3");
  EXPECT_EQ(matcher->GetMatch(event_2), 0);

  // events[1] matches. Returns 1.
  LabelerEvent event_3;
  event_3.set_person_country_code("COUNTRY_2");
  EXPECT_EQ(matcher->GetMatch(event_3), 1);

  // events[1] matches. Returns 1.
  // person_region_code is ignored as not included in hash_field_mask.
  LabelerEvent event_4;
  event_4.set_person_country_code("COUNTRY_2");
  event_4.set_person_region_code("REGION_3");
  EXPECT_EQ(matcher->GetMatch(event_4), 1);

  // No matches. Returns -1.
  LabelerEvent event_5;
  event_5.set_person_country_code("COUNTRY_3");
  EXPECT_EQ(matcher->GetMatch(event_5), -1);
}

TEST(HashFieldMaskMatcherTest, TestUnsetField) {
  std::vector<const LabelerEvent*> events;
  LabelerEvent input_event_1;
  input_event_1.set_person_region_code("REGION_1");
  events.emplace_back(&input_event_1);
  LabelerEvent input_event_2;
  input_event_2.set_person_region_code("REGION_2");
  events.emplace_back(&input_event_2);

  FieldMask hash_field_mask =
      MakeFieldMask({"person_country_code", "person_region_code"});
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<HashFieldMaskMatcher> matcher,
                       HashFieldMaskMatcher::Build(events, hash_field_mask));

  // events[0] matches. Returns 0.
  LabelerEvent event_1;
  event_1.set_person_region_code("REGION_1");
  EXPECT_EQ(matcher->GetMatch(event_1), 0);

  // No matches. Returns -1.
  // When using hash field mask, to match an event with unset field, the event
  // being checked must have the same field unset.
  LabelerEvent event_2;
  event_2.set_person_country_code("COUNTRY_1");
  event_2.set_person_region_code("REGION_1");
  EXPECT_EQ(matcher->GetMatch(event_2), -1);
}

}  // namespace
}  // namespace wfa_virtual_people
