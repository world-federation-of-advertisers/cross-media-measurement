// Copyright 2026 The Cross-Media Measurement Authors
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

#include "common_cpp/testing/status_macros.h"
#include "common_cpp/testing/status_matchers.h"
#include "gmock/gmock.h"
#include "google/protobuf/text_format.h"
#include "gtest/gtest.h"
#include "wfa/virtual_people/common/event.pb.h"
#include "wfa/virtual_people/common/label.pb.h"
#include "wfa/virtual_people/common/model.pb.h"
#include "wfa/virtual_people/core/labeler/labeler.h"

namespace wfa_virtual_people {
namespace {

using ::wfa::IsOk;

TEST(Pass1ModeTest, RankedNodeEmitsPoolAssignment) {
  CompiledNode root;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestRankedNode"
        ranked_population_node {
          pools { population_offset: 100 total_population: 1000 }
          random_seed: "test-seed"
          ranked_size: 500
          unranked_mode: DISJOINT
        }
      )pb",
      &root));

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<Labeler> labeler, Labeler::Build(root));

  LabelerInput input;
  input.mutable_event_id()->set_publisher("test");
  input.mutable_event_id()->set_id("event-1");

  LabelerOutput output;
  EXPECT_THAT(labeler->Label(input, output, LabelingMode::kPoolIdentity),
              IsOk());

  // Pass-1 mode: should have pool_assignments, no people.
  EXPECT_EQ(output.people_size(), 0);
  ASSERT_EQ(output.pool_assignments_size(), 1);
  EXPECT_EQ(output.pool_assignments(0).pool_offset(), 100);
  EXPECT_EQ(output.pool_assignments(0).pool_size(), 1000);
  EXPECT_EQ(output.pool_assignments(0).ranked_size(), 500);
}

TEST(Pass1ModeTest, PopulationNodeNoOpInPass1Mode) {
  CompiledNode root;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestPopulationNode"
        population_node {
          pools { population_offset: 10 total_population: 1 }
          random_seed: "TestRandomSeed"
        }
      )pb",
      &root));

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<Labeler> labeler, Labeler::Build(root));

  LabelerInput input;
  input.mutable_event_id()->set_publisher("test");
  input.mutable_event_id()->set_id("event-1");

  // PopulationNode no-ops in pool-identity mode — succeeds with no output.
  LabelerOutput output;
  EXPECT_THAT(labeler->Label(input, output, LabelingMode::kPoolIdentity),
              IsOk());
  EXPECT_EQ(output.people_size(), 0);
  EXPECT_EQ(output.pool_assignments_size(), 0);
}

TEST(Pass1ModeTest, DefaultModeUnchanged) {
  CompiledNode root;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestRankedNode"
        ranked_population_node {
          pools { population_offset: 100 total_population: 1000 }
          random_seed: "test-seed"
          ranked_size: 500
          unranked_mode: DISJOINT
        }
      )pb",
      &root));

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<Labeler> labeler, Labeler::Build(root));

  LabelerInput input;
  input.mutable_event_id()->set_publisher("test");
  input.mutable_event_id()->set_id("event-1");

  LabelerOutput output;
  // Default mode (kFull) — should assign a VID, no pool_assignments.
  EXPECT_THAT(labeler->Label(input, output), IsOk());
  EXPECT_EQ(output.people_size(), 1);
  EXPECT_TRUE(output.people(0).has_virtual_person_id());
  EXPECT_EQ(output.pool_assignments_size(), 0);
}

}  // namespace
}  // namespace wfa_virtual_people
