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

#include "wfa/virtual_people/core/model/model_serializer.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "common_cpp/testing/common_matchers.h"
#include "common_cpp/testing/status_macros.h"
#include "common_cpp/testing/status_matchers.h"
#include "gmock/gmock.h"
#include "google/protobuf/text_format.h"
#include "gtest/gtest.h"
#include "wfa/virtual_people/common/model.pb.h"

namespace wfa_virtual_people {
namespace {

using ::wfa::EqualsProto;
using ::wfa::StatusIs;

TEST(ModelSerializerTest, NoChildNode) {
  CompiledNode root;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestPopulationNode"
        population_node {
          pools { population_offset: 10 total_population: 3 }
          random_seed: "TestRandomSeed"
        }
      )pb",
      &root));

  std::vector<CompiledNode> expected;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestPopulationNode"
        index: 0
        population_node {
          pools { population_offset: 10 total_population: 3 }
          random_seed: "TestRandomSeed"
        }
      )pb",
      &expected.emplace_back()));

  ASSERT_OK_AND_ASSIGN(std::vector<wfa_virtual_people::CompiledNode> node_list,
                       ToNodeListRepresentation(root));
  EXPECT_THAT(node_list, testing::ElementsAre(EqualsProto(expected[0])));
}

TEST(ModelSerializerTest, WithChildNode) {
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
            chance: 0.3
          }
          branches {
            node {
              population_node {
                pools { population_offset: 30 total_population: 2 }
                random_seed: "TestPopulationNodeSeed3"
              }
            }
            chance: 0.3
          }
          random_seed: "TestBranchNodeSeed"
        }
      )pb",
      &root));

  std::vector<CompiledNode> expected;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        index: 0
        population_node {
          pools { population_offset: 10 total_population: 1 }
          random_seed: "TestPopulationNodeSeed1"
        }
      )pb",
      &expected.emplace_back()));
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        index: 1
        population_node {
          pools { population_offset: 20 total_population: 1 }
          random_seed: "TestPopulationNodeSeed2"
        }
      )pb",
      &expected.emplace_back()));
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        index: 2
        population_node {
          pools { population_offset: 30 total_population: 2 }
          random_seed: "TestPopulationNodeSeed3"
        }
      )pb",
      &expected.emplace_back()));
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestNode1"
        index: 3
        branch_node {
          branches { node_index: 0 chance: 0.4 }
          branches { node_index: 1 chance: 0.3 }
          branches { node_index: 2 chance: 0.3 }
          random_seed: "TestBranchNodeSeed"
        }
      )pb",
      &expected.emplace_back()));

  ASSERT_OK_AND_ASSIGN(std::vector<wfa_virtual_people::CompiledNode> node_list,
                       ToNodeListRepresentation(root));
  EXPECT_THAT(
      node_list,
      testing::ElementsAre(EqualsProto(expected[0]), EqualsProto(expected[1]),
                           EqualsProto(expected[2]), EqualsProto(expected[3])));
}

TEST(ModelSerializerTest, WithMultipleLevels) {
  CompiledNode root;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestNode1"
        branch_node {
          branches {
            node {
              name: "Branch1"
              branch_node {
                random_seed: "Branch1Seed"
                branches {
                  node {
                    population_node {
                      pools { population_offset: 100 total_population: 1 }
                      random_seed: "TestPopulationNodeSeed1"
                    }
                  }
                  chance: 0.4
                }
                branches {
                  node {
                    population_node {
                      pools { population_offset: 200 total_population: 1 }
                      random_seed: "TestPopulationNodeSeed2"
                    }
                  }
                  chance: 0.6
                }
              }
            }
            chance: 0.5
          }
          branches {
            node {
              name: "Branch2"
              branch_node {
                random_seed: "Branch2Seed"
                branches {
                  node {
                    population_node {
                      pools { population_offset: 300 total_population: 1 }
                      random_seed: "TestPopulationNodeSeed3"
                    }
                  }
                  chance: 0.3
                }
                branches {
                  node {
                    population_node {
                      pools { population_offset: 400 total_population: 1 }
                      random_seed: "TestPopulationNodeSeed4"
                    }
                  }
                  chance: 0.7
                }
              }
            }
            chance: 0.5
          }
          random_seed: "TestBranchNodeSeed"
        }
      )pb",
      &root));

  std::vector<CompiledNode> expected;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        index: 0
        population_node {
          pools { population_offset: 100 total_population: 1 }
          random_seed: "TestPopulationNodeSeed1"
        }
      )pb",
      &expected.emplace_back()));
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        index: 1
        population_node {
          pools { population_offset: 200 total_population: 1 }
          random_seed: "TestPopulationNodeSeed2"
        }
      )pb",
      &expected.emplace_back()));
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "Branch1"
        index: 2
        branch_node {
          branches { node_index: 0 chance: 0.4 }
          branches { node_index: 1 chance: 0.6 }
          random_seed: "Branch1Seed"
        }
      )pb",
      &expected.emplace_back()));
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        index: 3
        population_node {
          pools { population_offset: 300 total_population: 1 }
          random_seed: "TestPopulationNodeSeed3"
        }
      )pb",
      &expected.emplace_back()));
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        index: 4
        population_node {
          pools { population_offset: 400 total_population: 1 }
          random_seed: "TestPopulationNodeSeed4"
        }
      )pb",
      &expected.emplace_back()));
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "Branch2"
        index: 5
        branch_node {
          branches { node_index: 3 chance: 0.3 }
          branches { node_index: 4 chance: 0.7 }
          random_seed: "Branch2Seed"
        }
      )pb",
      &expected.emplace_back()));
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestNode1"
        index: 6
        branch_node {
          branches { node_index: 2 chance: 0.5 }
          branches { node_index: 5 chance: 0.5 }
          random_seed: "TestBranchNodeSeed"
        }
      )pb",
      &expected.emplace_back()));

  ASSERT_OK_AND_ASSIGN(std::vector<wfa_virtual_people::CompiledNode> node_list,
                       ToNodeListRepresentation(root));
  EXPECT_THAT(node_list, testing::ElementsAre(
                             EqualsProto(expected[0]), EqualsProto(expected[1]),
                             EqualsProto(expected[2]), EqualsProto(expected[3]),
                             EqualsProto(expected[4]), EqualsProto(expected[5]),
                             EqualsProto(expected[6])));
}

TEST(ModelSerializerTest, HasNodeIndex) {
  // Single node representation shouldn't use node_index.
  CompiledNode root;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestNode1"
        branch_node {
          branches { node_index: 1 chance: 1.0 }
          random_seed: "TestBranchNodeSeed"
        }
      )pb",
      &root));

  EXPECT_THAT(
      ToNodeListRepresentation(root).status(),
      StatusIs(absl::StatusCode::kInvalidArgument, "shouldn't use node_index"));
}

TEST(ModelSerializerTest, BranchChildNodeNotSet) {
  // Branch should have child node set.
  CompiledNode root;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestNode1"
        branch_node {
          branches { chance: 1.0 }
          random_seed: "TestBranchNodeSeed"
        }
      )pb",
      &root));

  EXPECT_THAT(
      ToNodeListRepresentation(root).status(),
      StatusIs(absl::StatusCode::kInvalidArgument, "child_node is not set"));
}

}  // namespace
}  // namespace wfa_virtual_people
