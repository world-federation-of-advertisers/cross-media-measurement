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

#include "wfa/virtual_people/core/labeler/labeler.h"

#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "common_cpp/testing/status_macros.h"
#include "common_cpp/testing/status_matchers.h"
#include "gmock/gmock.h"
#include "google/protobuf/text_format.h"
#include "gtest/gtest.h"
#include "wfa/virtual_people/common/event.pb.h"
#include "wfa/virtual_people/common/label.pb.h"
#include "wfa/virtual_people/common/model.pb.h"

namespace wfa_virtual_people {
namespace {

using ::testing::DoubleNear;
using ::testing::Pair;
using ::testing::UnorderedElementsAre;
using ::wfa::IsOk;
using ::wfa::StatusIs;

constexpr int kEventIdNumber = 10000;

TEST(LabelerTest, TestBuildFromRoot) {
  // A model with
  // * 40% probability to assign virtual person id 10
  // * 60% probability to assign virtual person id 20
  CompiledNode root;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestNode1"
        branch_node {
          branches {
            node {
              population_node {
                pools { population_offset: 10 total_population: 1 }
                random_seed: "TestPopulationNodeSeed1"
              }
            }
            chance: 0.4
          }
          branches {
            node {
              population_node {
                pools { population_offset: 20 total_population: 1 }
                random_seed: "TestPopulationNodeSeed2"
              }
            }
            chance: 0.6
          }
          random_seed: "TestBranchNodeSeed"
        }
      )pb",
      &root));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<Labeler> labeler, Labeler::Build(root));

  absl::flat_hash_map<uint64_t, int32_t> vpid_counts;
  for (int event_id = 0; event_id < kEventIdNumber; ++event_id) {
    LabelerInput input;
    input.mutable_event_id()->set_id(std::to_string(event_id));
    LabelerOutput output;
    EXPECT_THAT(labeler->Label(input, output), IsOk());
    ++vpid_counts[output.people(0).virtual_person_id()];
  }

  // Compare to the exact result to make sure C++ and Kotlin implementations
  // behave the same. The expected count for 10 is 0.4 * kEventIdNumber = 4000,
  // the expected count for 20 is 0.6 * kEventIdNumber = 6000
  EXPECT_THAT(vpid_counts,
              UnorderedElementsAre(Pair(10, 4061), Pair(20, 5939)));
}

TEST(LabelerTest, TestBuildFromNodesRootWithIndex) {
  // All nodes, including root node, have index set.
  // A model with
  // * 40% probability to assign virtual person id 10
  // * 60% probability to assign virtual person id 20
  std::vector<CompiledNode> nodes;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        index: 2
        name: "TestNode2"
        population_node {
          pools { population_offset: 10 total_population: 1 }
          random_seed: "TestPopulationNodeSeed1"
        }
      )pb",
      &nodes.emplace_back()));
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        index: 3
        name: "TestNode3"
        population_node {
          pools { population_offset: 20 total_population: 1 }
          random_seed: "TestPopulationNodeSeed2"
        }
      )pb",
      &nodes.emplace_back()));
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        index: 1
        name: "TestNode1"
        branch_node {
          branches { node_index: 2 chance: 0.4 }
          branches { node_index: 3 chance: 0.6 }
          random_seed: "TestBranchNodeSeed"
        }
      )pb",
      &nodes.emplace_back()));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<Labeler> labeler, Labeler::Build(nodes));

  absl::flat_hash_map<uint64_t, int32_t> vpid_counts;
  for (int event_id = 0; event_id < kEventIdNumber; ++event_id) {
    LabelerInput input;
    input.mutable_event_id()->set_id(std::to_string(event_id));
    LabelerOutput output;
    EXPECT_THAT(labeler->Label(input, output), IsOk());
    ++vpid_counts[output.people(0).virtual_person_id()];
  }

  // Compare to the exact result to make sure C++ and Kotlin implementations
  // behave the same. The expected count for 10 is 0.4 * kEventIdNumber = 4000,
  // the expected count for 20 is 0.6 * kEventIdNumber = 6000
  EXPECT_THAT(vpid_counts,
              UnorderedElementsAre(Pair(10, 4061), Pair(20, 5939)));
}

TEST(LabelerTest, TestBuildFromNodesRootWithoutIndex) {
  // All nodes, except root node, have index set.
  // A model with
  // * 40% probability to assign virtual person id 10
  // * 60% probability to assign virtual person id 20
  std::vector<CompiledNode> nodes;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        index: 2
        name: "TestNode2"
        population_node {
          pools { population_offset: 10 total_population: 1 }
          random_seed: "TestPopulationNodeSeed1"
        }
      )pb",
      &nodes.emplace_back()));
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        index: 3
        name: "TestNode3"
        population_node {
          pools { population_offset: 20 total_population: 1 }
          random_seed: "TestPopulationNodeSeed2"
        }
      )pb",
      &nodes.emplace_back()));
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestNode1"
        branch_node {
          branches { node_index: 2 chance: 0.4 }
          branches { node_index: 3 chance: 0.6 }
          random_seed: "TestBranchNodeSeed"
        }
      )pb",
      &nodes.emplace_back()));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<Labeler> labeler, Labeler::Build(nodes));

  absl::flat_hash_map<uint64_t, int32_t> vpid_counts;
  for (int event_id = 0; event_id < kEventIdNumber; ++event_id) {
    LabelerInput input;
    input.mutable_event_id()->set_id(std::to_string(event_id));
    LabelerOutput output;
    EXPECT_THAT(labeler->Label(input, output), IsOk());
    ++vpid_counts[output.people(0).virtual_person_id()];
  }

  // Compare to the exact result to make sure C++ and Kotlin implementations
  // behave the same. The expected count for 10 is 0.4 * kEventIdNumber = 4000,
  // the expected count for 20 is 0.6 * kEventIdNumber = 6000
  EXPECT_THAT(vpid_counts,
              UnorderedElementsAre(Pair(10, 4061), Pair(20, 5939)));
}

TEST(LabelerTest, TestBuildFromListWithSingleNode) {
  // A model with single node, which always assigns virtual person id 10.
  std::vector<CompiledNode> nodes;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestNode1"
        population_node {
          pools { population_offset: 10 total_population: 1 }
          random_seed: "TestPopulationNodeSeed1"
        }
      )pb",
      &nodes.emplace_back()));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<Labeler> labeler, Labeler::Build(nodes));

  for (int event_id = 0; event_id < kEventIdNumber; ++event_id) {
    LabelerInput input;
    input.mutable_event_id()->set_id(std::to_string(event_id));
    LabelerOutput output;
    EXPECT_THAT(labeler->Label(input, output), IsOk());
    EXPECT_EQ(output.people(0).virtual_person_id(), 10);
  }
}

TEST(LabelerTest, TestBuildFromNodesNodeAfterRoot) {
  std::vector<CompiledNode> nodes;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        index: 3
        name: "TestNode3"
        population_node {
          pools { population_offset: 20 total_population: 1 }
          random_seed: "TestPopulationNodeSeed2"
        }
      )pb",
      &nodes.emplace_back()));
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestNode1"
        branch_node {
          branches { node_index: 3 chance: 1 }
          random_seed: "TestBranchNodeSeed"
        }
      )pb",
      &nodes.emplace_back()));
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestNode2"
        population_node {
          pools { population_offset: 10 total_population: 1 }
          random_seed: "TestPopulationNodeSeed1"
        }
      )pb",
      &nodes.emplace_back()));
  EXPECT_THAT(Labeler::Build(nodes).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(LabelerTest, TestBuildFromNodesMultipleRoots) {
  // 2 trees exist:
  // 1 -> 3, 4
  // 2 -> 5
  std::vector<CompiledNode> nodes;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        index: 3
        name: "TestNode3"
        population_node {
          pools { population_offset: 10 total_population: 1 }
          random_seed: "TestPopulationNodeSeed1"
        }
      )pb",
      &nodes.emplace_back()));
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        index: 4
        name: "TestNode4"
        population_node {
          pools { population_offset: 20 total_population: 1 }
          random_seed: "TestPopulationNodeSeed2"
        }
      )pb",
      &nodes.emplace_back()));
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        index: 5
        name: "TestNode5"
        population_node {
          pools { population_offset: 20 total_population: 1 }
          random_seed: "TestPopulationNodeSeed3"
        }
      )pb",
      &nodes.emplace_back()));
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        index: 1
        name: "TestNode1"
        branch_node {
          branches { node_index: 3 chance: 0.4 }
          branches { node_index: 4 chance: 0.6 }
          random_seed: "TestBranchNodeSeed1"
        }
      )pb",
      &nodes.emplace_back()));
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        index: 2
        name: "TestNode2"
        branch_node {
          branches { node_index: 5 chance: 1 }
          random_seed: "TestBranchNodeSeed2"
        }
      )pb",
      &nodes.emplace_back()));
  EXPECT_THAT(Labeler::Build(nodes).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(LabelerTest, TestBuildFromNodesNoRoot) {
  // Nodes 1 and 2 reference each other as child node. No root node can be
  // recognized.
  std::vector<CompiledNode> nodes;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        index: 1
        name: "TestNode1"
        branch_node {
          branches { node_index: 2 chance: 1 }
          random_seed: "TestBranchNodeSeed1"
        }
      )pb",
      &nodes.emplace_back()));
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        index: 2
        name: "TestNode2"
        branch_node {
          branches { node_index: 1 chance: 1 }
          random_seed: "TestBranchNodeSeed2"
        }
      )pb",
      &nodes.emplace_back()));
  EXPECT_THAT(Labeler::Build(nodes).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(LabelerTest, TestBuildFromNodesNoNodeForIndex) {
  std::vector<CompiledNode> nodes;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        index: 1
        name: "TestNode1"
        branch_node {
          branches { node_index: 2 chance: 1 }
          random_seed: "TestBranchNodeSeed1"
        }
      )pb",
      &nodes.emplace_back()));
  EXPECT_THAT(Labeler::Build(nodes).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(LabelerTest, TestBuildFromNodesDuplicatedIndexes) {
  std::vector<CompiledNode> nodes;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        index: 2
        name: "TestNode2"
        population_node {
          pools { population_offset: 10 total_population: 1 }
          random_seed: "TestPopulationNodeSeed1"
        }
      )pb",
      &nodes.emplace_back()));
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        index: 2
        name: "TestNode3"
        population_node {
          pools { population_offset: 20 total_population: 1 }
          random_seed: "TestPopulationNodeSeed2"
        }
      )pb",
      &nodes.emplace_back()));
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestNode1"
        branch_node {
          branches { node_index: 2 chance: 1 }
          random_seed: "TestBranchNodeSeed1"
        }
      )pb",
      &nodes.emplace_back()));
  EXPECT_THAT(Labeler::Build(nodes).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(LabelerTest, TestBuildFromNodesMultipleParents) {
  // This is invalid as node 3 is the child node of both node 1 and node 2.
  std::vector<CompiledNode> nodes;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        index: 3
        name: "TestNode3"
        population_node {
          pools { population_offset: 10 total_population: 1 }
          random_seed: "TestPopulationNodeSeed1"
        }
      )pb",
      &nodes.emplace_back()));
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        index: 2
        name: "TestNode2"
        branch_node {
          branches { node_index: 3 chance: 1 }
          random_seed: "TestBranchNodeSeed2"
        }
      )pb",
      &nodes.emplace_back()));
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestNode1"
        branch_node {
          branches { node_index: 2 chance: 0.5 }
          branches { node_index: 3 chance: 0.5 }
          random_seed: "TestBranchNodeSeed1"
        }
      )pb",
      &nodes.emplace_back()));
  EXPECT_THAT(Labeler::Build(nodes).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

namespace {

// Returns a minimal single-pool model used by the debug-trace tests below.
absl::StatusOr<std::unique_ptr<Labeler>> BuildSinglePoolLabeler() {
  CompiledNode root;
  if (!google::protobuf::TextFormat::ParseFromString(
          R"pb(
            name: "TestNode1"
            branch_node {
              branches {
                node {
                  population_node {
                    pools { population_offset: 10 total_population: 1 }
                    random_seed: "TestPopulationNodeSeed1"
                  }
                }
                chance: 1.0
              }
              random_seed: "TestBranchNodeSeed"
            }
          )pb",
          &root)) {
    return absl::InternalError("failed to parse test model");
  }
  return Labeler::Build(root);
}

}  // namespace

TEST(LabelerTest, SerializedDebugTraceEmptyWhenEnableDebugTraceUnset) {
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<Labeler> labeler,
                       BuildSinglePoolLabeler());
  LabelerInput input;
  input.mutable_event_id()->set_id("evt_42");
  // Default: enable_debug_trace is unset (== false).
  LabelerOutput output;
  EXPECT_THAT(labeler->Label(input, output), IsOk());
  EXPECT_EQ(output.serialized_debug_trace(), "");
}

TEST(LabelerTest, SerializedDebugTraceEmptyWhenEnableDebugTraceFalse) {
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<Labeler> labeler,
                       BuildSinglePoolLabeler());
  LabelerInput input;
  input.mutable_event_id()->set_id("evt_42");
  input.set_enable_debug_trace(false);
  LabelerOutput output;
  EXPECT_THAT(labeler->Label(input, output), IsOk());
  EXPECT_EQ(output.serialized_debug_trace(), "");
}

TEST(LabelerTest, SerializedDebugTracePopulatedWhenEnableDebugTraceTrue) {
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<Labeler> labeler,
                       BuildSinglePoolLabeler());
  LabelerInput input;
  input.mutable_event_id()->set_id("evt_42");
  input.set_enable_debug_trace(true);
  LabelerOutput output;
  EXPECT_THAT(labeler->Label(input, output), IsOk());
  EXPECT_FALSE(output.serialized_debug_trace().empty());
  // The trace is the DebugString() of the LabelerEvent, so the input event
  // id we set should be visible verbatim in the trace.
  EXPECT_NE(output.serialized_debug_trace().find("evt_42"), std::string::npos);
  // And the trace should be re-parseable as a LabelerEvent text format,
  // round-tripping back to the original input event id.
  LabelerEvent rebuilt;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      output.serialized_debug_trace(), &rebuilt));
  EXPECT_EQ(rebuilt.labeler_input().event_id().id(), "evt_42");
}

}  // namespace
}  // namespace wfa_virtual_people
