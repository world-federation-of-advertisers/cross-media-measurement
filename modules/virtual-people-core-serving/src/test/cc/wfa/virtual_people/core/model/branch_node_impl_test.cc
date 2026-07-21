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

#include "wfa/virtual_people/core/model/branch_node_impl.h"

#include <memory>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/string_view.h"
#include "common_cpp/testing/status_macros.h"
#include "common_cpp/testing/status_matchers.h"
#include "gmock/gmock.h"
#include "google/protobuf/text_format.h"
#include "gtest/gtest.h"
#include "wfa/virtual_people/common/label.pb.h"
#include "wfa/virtual_people/common/model.pb.h"
#include "wfa/virtual_people/core/model/attributes_updater.h"
#include "wfa/virtual_people/core/model/model_node.h"
#include "wfa/virtual_people/core/model/multiplicity_impl.h"

namespace wfa_virtual_people {
namespace {

using ::testing::AnyOf;
using ::testing::Pair;
using ::testing::UnorderedElementsAre;
using ::wfa::IsOk;
using ::wfa::StatusIs;

constexpr int kFingerprintNumber = 10000;

TEST(BranchNodeImplTest, TestApplyBranchWithNodeByChance) {
  // The branch node has 2 branches.
  // One branch is selected with 40% chance, which is a population node always
  // assigns virtual person id 10.
  // The other branch is selected with 60% chance, which is a population node
  // always assigns virtual person id 20.
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestBranchNode"
        index: 1
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
                random_seed: "TestPopulationNodeSeed1"
              }
            }
            chance: 0.6
          }
          random_seed: "TestBranchNodeSeed"
        }
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<ModelNode> node,
                       ModelNode::Build(config));

  absl::flat_hash_map<uint64_t, int32_t> id_counts;
  for (int fingerprint = 0; fingerprint < kFingerprintNumber; ++fingerprint) {
    LabelerEvent input;
    input.set_acting_fingerprint(fingerprint);
    EXPECT_THAT(node->Apply(input), IsOk());
    ++id_counts[input.virtual_person_activities(0).virtual_person_id()];
  }

  // Compare to the exact result to make sure C++ and Kotlin implementations
  // behave the same.
  // The expected count for 10 is 0.4 * kFingerprintNumber = 4000
  // The expected count for 20 is 0.6 * kFingerprintNumber = 6000
  EXPECT_THAT(id_counts, UnorderedElementsAre(Pair(10, 4014), Pair(20, 5986)));
}

TEST(BranchNodeImplTest, TestApplyBranchWithNodeIndexByChance) {
  // The branch node has 2 branches.
  // One branch is selected with 40% chance, which is a population node always
  // assigns virtual person id 10.
  // The other branch is selected with 60% chance, which is a population node
  // always assigns virtual person id 20.
  CompiledNode branch_node_config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestBranchNode"
        index: 1
        branch_node {
          branches { node_index: 2 chance: 0.4 }
          branches { node_index: 3 chance: 0.6 }
          random_seed: "TestBranchNodeSeed"
        }
      )pb",
      &branch_node_config));

  CompiledNode population_node_config_1;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestPopulationNode1"
        index: 2
        population_node {
          pools { population_offset: 10 total_population: 1 }
          random_seed: "TestPopulationNodeSeed1"
        }
      )pb",
      &population_node_config_1));

  CompiledNode population_node_config_2;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestPopulationNode2"
        index: 3
        population_node {
          pools { population_offset: 20 total_population: 1 }
          random_seed: "TestPopulationNodeSeed2"
        }
      )pb",
      &population_node_config_2));

  // Set up map from indexes to child nodes.
  absl::flat_hash_map<uint32_t, std::unique_ptr<ModelNode>> node_refs;
  ASSERT_OK_AND_ASSIGN(node_refs[2],
                       ModelNode::Build(population_node_config_1));
  ASSERT_OK_AND_ASSIGN(node_refs[3],
                       ModelNode::Build(population_node_config_2));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<ModelNode> branch_node,
                       ModelNode::Build(branch_node_config, node_refs));

  absl::flat_hash_map<uint64_t, int32_t> id_counts;
  for (int fingerprint = 0; fingerprint < kFingerprintNumber; ++fingerprint) {
    LabelerEvent input;
    input.set_acting_fingerprint(fingerprint);
    EXPECT_THAT(branch_node->Apply(input), IsOk());
    ++id_counts[input.virtual_person_activities(0).virtual_person_id()];
  }

  // Compare to the exact result to make sure C++ and Kotlin implementations
  // behave the same.
  // The expected count for 10 is 0.4 * kFingerprintNumber = 4000
  // The expected count for 20 is 0.6 * kFingerprintNumber = 6000
  EXPECT_THAT(id_counts, UnorderedElementsAre(Pair(10, 4014), Pair(20, 5986)));
}

TEST(BranchNodeImplTest, TestBranchWithNodeIndexByChanceNotNormalized) {
  // The branch node has 2 branches, but the chances are not normalized.
  CompiledNode branch_node_config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestBranchNode"
        index: 1
        branch_node {
          branches { node_index: 2 chance: 0.8 }
          branches { node_index: 3 chance: 0.12 }
          random_seed: "TestBranchNodeSeed"
        }
      )pb",
      &branch_node_config));

  CompiledNode population_node_config_1;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestPopulationNode1"
        index: 2
        population_node {
          pools { population_offset: 10 total_population: 1 }
          random_seed: "TestPopulationNodeSeed1"
        }
      )pb",
      &population_node_config_1));

  CompiledNode population_node_config_2;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestPopulationNode2"
        index: 3
        population_node {
          pools { population_offset: 20 total_population: 1 }
          random_seed: "TestPopulationNodeSeed2"
        }
      )pb",
      &population_node_config_2));

  // Set up map from indexes to child nodes.
  absl::flat_hash_map<uint32_t, std::unique_ptr<ModelNode>> node_refs;
  ASSERT_OK_AND_ASSIGN(node_refs[2],
                       ModelNode::Build(population_node_config_1));
  ASSERT_OK_AND_ASSIGN(node_refs[3],
                       ModelNode::Build(population_node_config_2));
  EXPECT_THAT(ModelNode::Build(branch_node_config, node_refs).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(BranchNodeImplTest, TestApplyBranchWithNodeByCondition) {
  // The branch node has 2 branches.
  // One branch is selected if person_country_code is "country_code_1", which is
  // a population node always assigns virtual person id 10.
  // The other branch is selected if person_country_code is "country_code_2",
  // which is a population node always assigns virtual person id 20.
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestBranchNode"
        index: 1
        branch_node {
          branches {
            node {
              population_node {
                pools { population_offset: 10 total_population: 1 }
                random_seed: "TestPopulationNodeSeed1"
              }
            }
            condition {
              name: "person_country_code"
              op: EQUAL
              value: "country_code_1"
            }
          }
          branches {
            node {
              population_node {
                pools { population_offset: 20 total_population: 1 }
                random_seed: "TestPopulationNodeSeed1"
              }
            }
            condition {
              name: "person_country_code"
              op: EQUAL
              value: "country_code_2"
            }
          }
        }
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<ModelNode> node,
                       ModelNode::Build(config));

  LabelerEvent input_1;
  input_1.set_person_country_code("country_code_1");
  EXPECT_THAT(node->Apply(input_1), IsOk());
  EXPECT_EQ(input_1.virtual_person_activities(0).virtual_person_id(), 10);

  LabelerEvent input_2;
  input_2.set_person_country_code("country_code_2");
  EXPECT_THAT(node->Apply(input_2), IsOk());
  EXPECT_EQ(input_2.virtual_person_activities(0).virtual_person_id(), 20);

  // No branch matches. Returns error status.
  LabelerEvent input_3;
  input_3.set_person_country_code("country_code_3");
  EXPECT_THAT(node->Apply(input_3),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(BranchNodeImplTest, TestApplyBranchWithNodeIndexResolvedRecursively) {
  // The branch node has 2 branches.
  // One branch is selected with 40% chance, which is a branch node refers to a
  // single population node always assigning virtual person id 10.
  // The other branch is selected with 60% chance, which is a branch node refers
  // to a single population node always assigning virtual person id 20.
  CompiledNode branch_node_config_1;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestBranchNode1"
        index: 1
        branch_node {
          branches { node_index: 2 chance: 0.4 }
          branches { node_index: 3 chance: 0.6 }
          random_seed: "TestBranchNodeSeed1"
        }
      )pb",
      &branch_node_config_1));

  CompiledNode branch_node_config_2;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestBranchNode2"
        index: 2
        branch_node {
          branches { node_index: 4 chance: 1 }
          random_seed: "TestBranchNodeSeed2"
        }
      )pb",
      &branch_node_config_2));

  CompiledNode branch_node_config_3;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestBranchNode3"
        index: 3
        branch_node {
          branches { node_index: 5 chance: 1 }
          random_seed: "TestBranchNodeSeed3"
        }
      )pb",
      &branch_node_config_3));

  CompiledNode population_node_config_1;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestPopulationNode1"
        index: 4
        population_node {
          pools { population_offset: 10 total_population: 1 }
          random_seed: "TestPopulationNodeSeed1"
        }
      )pb",
      &population_node_config_1));

  CompiledNode population_node_config_2;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestPopulationNode2"
        index: 5
        population_node {
          pools { population_offset: 20 total_population: 1 }
          random_seed: "TestPopulationNodeSeed2"
        }
      )pb",
      &population_node_config_2));

  // Set up map from indexes to child nodes.
  absl::flat_hash_map<uint32_t, std::unique_ptr<ModelNode>> node_refs;
  ASSERT_OK_AND_ASSIGN(node_refs[4],
                       ModelNode::Build(population_node_config_1));
  ASSERT_OK_AND_ASSIGN(node_refs[2],
                       ModelNode::Build(branch_node_config_2, node_refs));
  ASSERT_OK_AND_ASSIGN(node_refs[5],
                       ModelNode::Build(population_node_config_2));
  ASSERT_OK_AND_ASSIGN(node_refs[3],
                       ModelNode::Build(branch_node_config_3, node_refs));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<ModelNode> branch_node_1,
                       ModelNode::Build(branch_node_config_1, node_refs));

  absl::flat_hash_map<uint64_t, int32_t> id_counts;
  for (int fingerprint = 0; fingerprint < kFingerprintNumber; ++fingerprint) {
    LabelerEvent input;
    input.set_acting_fingerprint(fingerprint);
    EXPECT_THAT(branch_node_1->Apply(input), IsOk());
    ++id_counts[input.virtual_person_activities(0).virtual_person_id()];
  }

  // Compare to the exact result to make sure C++ and Kotlin implementations
  // behave the same.
  // The expected count for 10 is 0.4 * kFingerprintNumber = 4000
  // The expected count for 20 is 0.6 * kFingerprintNumber = 6000
  EXPECT_THAT(id_counts, UnorderedElementsAre(Pair(10, 4010), Pair(20, 5990)));
}

TEST(BranchNodeImplTest, TestNoBranch) {
  CompiledNode config;
  ASSERT_TRUE(
      google::protobuf::TextFormat::ParseFromString(R"pb(
                                                      name: "TestBranchNode"
                                                      index: 1
                                                      branch_node {}
                                                    )pb",
                                                    &config));
  EXPECT_THAT(ModelNode::Build(config).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(BranchNodeImplTest, TestNoChildNode) {
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestBranchNode"
        index: 1
        branch_node { branches { chance: 1 } }
      )pb",
      &config));
  EXPECT_THAT(ModelNode::Build(config).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(BranchNodeImplTest, TestNoSelectBy) {
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestBranchNode"
        index: 1
        branch_node {
          branches {
            node {
              population_node {
                pools { population_offset: 10 total_population: 1 }
                random_seed: "TestPopulationNodeSeed1"
              }
            }
          }
        }
      )pb",
      &config));
  EXPECT_THAT(ModelNode::Build(config).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(BranchNodeImplTest, TestDifferentSelectBy) {
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestBranchNode"
        index: 1
        branch_node {
          branches {
            node {
              population_node {
                pools { population_offset: 10 total_population: 1 }
                random_seed: "TestPopulationNodeSeed1"
              }
            }
            chance: 0.5
          }
          branches {
            node {
              population_node {
                pools { population_offset: 20 total_population: 1 }
                random_seed: "TestPopulationNodeSeed1"
              }
            }
            condition {}
          }
        }
      )pb",
      &config));
  EXPECT_THAT(ModelNode::Build(config).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(BranchNodeImplTest, TestResolveChildReferencesIndexNotFound) {
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestBranchNode"
        index: 1
        branch_node {
          branches { node_index: 2 chance: 1 }
          random_seed: "TestBranchNodeSeed"
        }
      )pb",
      &config));
  EXPECT_THAT(ModelNode::Build(config).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(BranchNodeImplTest, TestBothUpdatesAndMultiplicity) {
  // Failure case: have both updaters and multiplicity.
  // This won't happen from Build() because they are oneof.
  std::vector<std::unique_ptr<AttributesUpdaterInterface>> updaters;
  BranchNode::AttributesUpdater updater_config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        update_matrix {
          columns { person_country_code: "RAW_COUNTRY_1" }
          rows { person_country_code: "country_code_1" }
          probabilities: 1.0
        }
      )pb",
      &updater_config));
  ASSERT_OK_AND_ASSIGN(updaters.emplace_back(),
                       AttributesUpdaterInterface::Build(updater_config));

  std::unique_ptr<MultiplicityImpl> multiplicity;
  Multiplicity multiplicity_config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        expected_multiplicity: 1.2
        max_value: 2
        cap_at_max: true
        person_index_field: "multiplicity_person_index"
        random_seed: "test multiplicity"
      )pb",
      &multiplicity_config));
  ASSERT_OK_AND_ASSIGN(multiplicity,
                       MultiplicityImpl::Build(multiplicity_config));

  CompiledNode node_config;
  ASSERT_TRUE(
      google::protobuf::TextFormat::ParseFromString(R"pb(
                                                      name: "TestBranchNode"
                                                    )pb",
                                                    &node_config));

  std::unique_ptr<BranchNodeImpl> node = absl::make_unique<BranchNodeImpl>(
      node_config, std::vector<std::unique_ptr<ModelNode>>(), nullptr,
      "random_seed", nullptr, std::move(updaters), std::move(multiplicity));
  LabelerEvent event;
  EXPECT_THAT(node->Apply(event),
              StatusIs(absl::StatusCode::kInternal,
                       "cannot have both updaters and multiplicity"));
}

TEST(BranchNodeImplTest, TestApplyUpdateMatrix) {
  // The branch node has 1 attributes updater and 2 branches.
  // The update matrix is
  //                     "RAW_COUNTRY_1" "RAW_COUNTRY_2"
  // "country_code_1"         0.8             0.2
  // "country_code_2"         0.2             0.8
  //
  // One branch is selected if person_country_code is "country_code_1", which is
  // a population node always assigns virtual person id 10.
  // The other branch is selected if person_country_code is "country_code_2",
  // which is a population node always assigns virtual person id 20.
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestBranchNode"
        index: 1
        branch_node {
          branches {
            node {
              population_node {
                pools { population_offset: 10 total_population: 1 }
                random_seed: "TestPopulationNodeSeed1"
              }
            }
            condition {
              name: "person_country_code"
              op: EQUAL
              value: "country_code_1"
            }
          }
          branches {
            node {
              population_node {
                pools { population_offset: 20 total_population: 1 }
                random_seed: "TestPopulationNodeSeed1"
              }
            }
            condition {
              name: "person_country_code"
              op: EQUAL
              value: "country_code_2"
            }
          }
          updates {
            updates {
              update_matrix {
                columns { person_country_code: "RAW_COUNTRY_1" }
                columns { person_country_code: "RAW_COUNTRY_2" }
                rows { person_country_code: "country_code_1" }
                rows { person_country_code: "country_code_2" }
                probabilities: 0.8
                probabilities: 0.2
                probabilities: 0.2
                probabilities: 0.8
              }
            }
          }
        }
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<ModelNode> node,
                       ModelNode::Build(config));

  // Test for RAW_COUNTRY_1
  absl::flat_hash_map<uint64_t, int32_t> id_counts_1;
  for (int fingerprint = 0; fingerprint < kFingerprintNumber; ++fingerprint) {
    LabelerEvent input;
    input.set_person_country_code("RAW_COUNTRY_1");
    input.set_acting_fingerprint(fingerprint);
    ASSERT_THAT(node->Apply(input), IsOk());
    // Fingerprint is not changed.
    EXPECT_EQ(input.acting_fingerprint(), fingerprint);
    absl::string_view person_country_code = input.person_country_code();
    uint64_t id = input.virtual_person_activities(0).virtual_person_id();
    EXPECT_THAT(std::pair(person_country_code, id),
                AnyOf(Pair("country_code_1", 10), Pair("country_code_2", 20)));
    ++id_counts_1[id];
  }
  // The selected column is
  //                     "RAW_COUNTRY_1"
  // "country_code_1"         0.8
  // "country_code_2"         0.2
  // Compare to the exact result to make sure C++ and Kotlin implementations
  // behave the same.
  EXPECT_THAT(id_counts_1,
              UnorderedElementsAre(Pair(10, 8022), Pair(20, 1978)));

  // Test for RAW_COUNTRY_2
  absl::flat_hash_map<uint64_t, int32_t> id_counts_2;
  for (int fingerprint = 0; fingerprint < kFingerprintNumber; ++fingerprint) {
    LabelerEvent input;
    input.set_person_country_code("RAW_COUNTRY_2");
    input.set_acting_fingerprint(fingerprint);
    ASSERT_THAT(node->Apply(input), IsOk());
    absl::string_view person_country_code = input.person_country_code();
    uint64_t id = input.virtual_person_activities(0).virtual_person_id();
    EXPECT_THAT(std::pair(person_country_code, id),
                AnyOf(Pair("country_code_1", 10), Pair("country_code_2", 20)));
    ++id_counts_2[id];
  }
  // The selected column is
  //                     "RAW_COUNTRY_2"
  // "country_code_1"         0.2
  // "country_code_2"         0.8
  // Compare to the exact result to make sure C++ and Kotlin implementations
  // behave the same.
  EXPECT_THAT(id_counts_2,
              UnorderedElementsAre(Pair(10, 2064), Pair(20, 7936)));
}

TEST(BranchNodeImplTest, TestApplyUpdateMatricesInOrder) {
  // The branch node has 2 attributes updater and 1 branches.
  // The 1st update matrix always changes person_country_code from COUNTRY_1 to
  // COUNTRY_2.
  // The 2nd update matrix always changes person_country_code from COUNTRY_2 to
  // COUNTRY_3.
  //
  // One branch is selected if person_country_code is "COUNTRY_3", which is a
  // population node always assigns virtual person id 10.
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestBranchNode"
        index: 1
        branch_node {
          branches {
            node {
              population_node {
                pools { population_offset: 10 total_population: 1 }
                random_seed: "TestPopulationNodeSeed1"
              }
            }
            condition {
              name: "person_country_code"
              op: EQUAL
              value: "COUNTRY_3"
            }
          }
          updates {
            updates {
              update_matrix {
                columns { person_country_code: "COUNTRY_1" }
                rows { person_country_code: "COUNTRY_2" }
                probabilities: 1
              }
            }
            updates {
              update_matrix {
                columns { person_country_code: "COUNTRY_2" }
                rows { person_country_code: "COUNTRY_3" }
                probabilities: 1
              }
            }
          }
        }
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<ModelNode> node,
                       ModelNode::Build(config));

  // Test for COUNTRY_1
  for (int fingerprint = 0; fingerprint < kFingerprintNumber; ++fingerprint) {
    LabelerEvent input;
    input.set_person_country_code("COUNTRY_1");
    input.set_acting_fingerprint(fingerprint);
    ASSERT_THAT(node->Apply(input), IsOk());
    // Fingerprint is not changed.
    EXPECT_EQ(input.acting_fingerprint(), fingerprint);
    EXPECT_EQ(input.person_country_code(), "COUNTRY_3");
    EXPECT_EQ(input.virtual_person_activities(0).virtual_person_id(), 10);
  }
}

TEST(BranchNodeImplTest, TestInvalidMultiplicityField) {
  // multiplicity is not properly configured.
  // See multiplicity_impl_test for more test coverage on multiplicity.
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestBranchNode"
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
          multiplicity {}
        }
      )pb",
      &config));
  EXPECT_THAT(ModelNode::Build(config).status(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "must set multiplicity_ref"));
}

TEST(BranchNodeImplTest, TestExplicitMultiplicityAndCapAtMaxTrue) {
  // The branch node has 1 branch, which is a population node always
  // assigns virtual person id 10.
  // Use explicit multiplicity = 3.
  // When cap_at_max = true, should cap to max_value = 1.2.
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestBranchNode"
        index: 1
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
          multiplicity {
            expected_multiplicity: 3
            max_value: 1.2
            cap_at_max: true
            person_index_field: "multiplicity_person_index"
            random_seed: "test multiplicity"
          }
        }
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<ModelNode> node,
                       ModelNode::Build(config));

  // All virtual_person_id = 10.
  int64_t person_total = 0;
  for (int fingerprint = 0; fingerprint < kFingerprintNumber; ++fingerprint) {
    LabelerEvent input;
    input.set_acting_fingerprint(fingerprint);
    EXPECT_THAT(node->Apply(input), IsOk());
    EXPECT_THAT(input.virtual_person_activities().size(), AnyOf(1, 2));
    person_total += input.virtual_person_activities().size();
    for (auto& person : input.virtual_person_activities()) {
      EXPECT_EQ(person.virtual_person_id(), 10);
    }
  }

  // Compare to the exact result to make sure C++ and Kotlin implementations
  // behave the same. View is around 1.2 * kFingerprintNumber = 12000
  EXPECT_EQ(person_total, 11949);
}

TEST(BranchNodeImplTest, TestExplicitMultiplicityAndCapAtMaxFalse) {
  // The branch node has 1 branch, which is a population node always
  // assigns virtual person id 10.
  // Use explicit multiplicity = 3.
  // When cap_at_max = false, should return error.
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestBranchNode"
        index: 1
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
          multiplicity {
            expected_multiplicity: 3
            max_value: 1.2
            cap_at_max: false
            person_index_field: "multiplicity_person_index"
            random_seed: "test multiplicity"
          }
        }
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<ModelNode> node,
                       ModelNode::Build(config));

  for (int fingerprint = 0; fingerprint < kFingerprintNumber; ++fingerprint) {
    LabelerEvent input;
    input.set_acting_fingerprint(0);
    EXPECT_THAT(node->Apply(input),
                StatusIs(absl::StatusCode::kOutOfRange,
                         "exceeds the specified max value"));
  }
}

TEST(BranchNodeImplTest, TestMultiplicityFieldAndCapAtMaxTrue) {
  // The branch node has 1 branch, which is a population node always
  // assigns virtual person id 10.
  // Read multiplicity from field.
  // When cap_at_max = true, should cap to max_value = 1.2.
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestBranchNode"
        index: 1
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
          multiplicity {
            expected_multiplicity_field: "expected_multiplicity"
            max_value: 1.2
            cap_at_max: true
            person_index_field: "multiplicity_person_index"
            random_seed: "test multiplicity"
          }
        }
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<ModelNode> node,
                       ModelNode::Build(config));

  // multiplicity field is not set, return error
  for (int fingerprint = 0; fingerprint < kFingerprintNumber; ++fingerprint) {
    LabelerEvent input;
    input.set_acting_fingerprint(fingerprint);
    EXPECT_THAT(node->Apply(input), StatusIs(absl::StatusCode::kInvalidArgument,
                                             "multiplicity field is not set"));
  }

  // multiplicity > max_value, cap at max_value
  // All virtual_person_id = 10.
  int64_t person_total = 0;
  for (int fingerprint = 0; fingerprint < kFingerprintNumber; ++fingerprint) {
    LabelerEvent input;
    input.set_acting_fingerprint(fingerprint);
    input.set_expected_multiplicity(2);
    EXPECT_THAT(node->Apply(input), IsOk());
    EXPECT_THAT(input.virtual_person_activities().size(), AnyOf(1, 2));
    person_total += input.virtual_person_activities().size();
    for (auto& person : input.virtual_person_activities()) {
      EXPECT_EQ(person.virtual_person_id(), 10);
    }
  }
  // Compare to the exact result to make sure C++ and Kotlin implementations
  // behave the same. View is around 1.2 * kFingerprintNumber = 12000
  EXPECT_EQ(person_total, 11949);

  // multiplicity < max_value
  // All virtual_person_id = 10.
  person_total = 0;
  for (int fingerprint = 0; fingerprint < kFingerprintNumber; ++fingerprint) {
    LabelerEvent input;
    input.set_acting_fingerprint(fingerprint);
    input.set_expected_multiplicity(1.1);
    EXPECT_THAT(node->Apply(input), IsOk());
    EXPECT_THAT(input.virtual_person_activities().size(), AnyOf(1, 2));
    person_total += input.virtual_person_activities().size();
    for (auto& person : input.virtual_person_activities()) {
      EXPECT_EQ(person.virtual_person_id(), 10);
    }
  }
  // Compare to the exact result to make sure C++ and Kotlin implementations
  // behave the same. View is around 1.1 * kFingerprintNumber = 11000
  EXPECT_EQ(person_total, 10959);
}

TEST(BranchNodeImplTest, TestMultiplicityFieldAndCapAtMaxFalse) {
  // The branch node has 1 branch, which is a population node always
  // assigns virtual person id 10.
  // Read multiplicity from field.
  // When cap_at_max = false, should return error if > max_value.
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestBranchNode"
        index: 1
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
          multiplicity {
            expected_multiplicity_field: "expected_multiplicity"
            max_value: 1.2
            cap_at_max: false
            person_index_field: "multiplicity_person_index"
            random_seed: "test multiplicity"
          }
        }
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<ModelNode> node,
                       ModelNode::Build(config));

  // multiplicity field is not set, return error
  for (int fingerprint = 0; fingerprint < kFingerprintNumber; ++fingerprint) {
    LabelerEvent input;
    input.set_acting_fingerprint(fingerprint);
    EXPECT_THAT(node->Apply(input), StatusIs(absl::StatusCode::kInvalidArgument,
                                             "multiplicity field is not set"));
  }

  // multiplicity > max_value, return error
  for (int fingerprint = 0; fingerprint < kFingerprintNumber; ++fingerprint) {
    LabelerEvent input;
    input.set_acting_fingerprint(fingerprint);
    input.set_expected_multiplicity(1.5);
    EXPECT_THAT(node->Apply(input),
                StatusIs(absl::StatusCode::kOutOfRange,
                         "exceeds the specified max value"));
  }

  // multiplicity < max_value
  // All virtual_person_id = 10.
  int64_t person_total = 0;
  for (int fingerprint = 0; fingerprint < kFingerprintNumber; ++fingerprint) {
    LabelerEvent input;
    input.set_acting_fingerprint(fingerprint);
    input.set_expected_multiplicity(1.1);
    EXPECT_THAT(node->Apply(input), IsOk());
    EXPECT_THAT(input.virtual_person_activities().size(), AnyOf(1, 2));
    person_total += input.virtual_person_activities().size();
    for (auto& person : input.virtual_person_activities()) {
      EXPECT_EQ(person.virtual_person_id(), 10);
    }
  }
  // Compare to the exact result to make sure C++ and Kotlin implementations
  // behave the same. View is around 1.1 * kFingerprintNumber = 11000
  EXPECT_EQ(person_total, 10959);
}

TEST(BranchNodeImplTest, TestExplicitMultiplicity) {
  // The branch node has 2 branches.
  // One branch is selected with 20% chance, which is a population node always
  // assigns virtual person id 10.
  // The other branch is selected with 80% chance, which is a population node
  // always assigns virtual person id 20.
  // Multiplicity is explicit value = 1.3.
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestBranchNode"
        index: 1
        branch_node {
          branches {
            node {
              population_node {
                pools { population_offset: 10 total_population: 1 }
                random_seed: "TestPopulationNodeSeed1"
              }
            }
            chance: 0.2
          }
          branches {
            node {
              population_node {
                pools { population_offset: 20 total_population: 1 }
                random_seed: "TestPopulationNodeSeed2"
              }
            }
            chance: 0.8
          }
          random_seed: "TestBranchNodeSeed"
          multiplicity {
            expected_multiplicity: 1.3
            max_value: 2
            cap_at_max: true
            person_index_field: "multiplicity_person_index"
            random_seed: "test multiplicity"
          }
        }
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<ModelNode> node,
                       ModelNode::Build(config));

  absl::flat_hash_map<uint64_t, int32_t> id_counts_index_0;
  absl::flat_hash_map<uint64_t, int32_t> id_counts_index_1;
  int64_t same_pool = 0;
  int64_t different_pool = 0;
  for (int fingerprint = 0; fingerprint < kFingerprintNumber; ++fingerprint) {
    LabelerEvent input;
    input.set_acting_fingerprint(fingerprint);
    EXPECT_THAT(node->Apply(input), IsOk());
    EXPECT_THAT(input.virtual_person_activities().size(), AnyOf(1, 2));
    ++id_counts_index_0[input.virtual_person_activities(0).virtual_person_id()];
    if (input.virtual_person_activities().size() == 2) {
      ++id_counts_index_1[input.virtual_person_activities(1)
                              .virtual_person_id()];
      if (input.virtual_person_activities(0).virtual_person_id() ==
          input.virtual_person_activities(1).virtual_person_id()) {
        ++same_pool;
      } else {
        ++different_pool;
      }
    }
  }

  // Expect ~70% events have 1 virtual person, ~30% events have 2.
  // The clones are labeled independently, so they have the same probability
  // to go to each pool.
  // Expected values are
  //   idCountsIndex0[10] ~= kFingerprintNumber * 0.8 = 8000
  //   idCountsIndex0[20] ~= kFingerprintNumber * 0.2 = 2000
  //   idCountsIndex1[10] ~= kFingerprintNumber * 0.3 * 0.2 = 600
  //   idCountsIndex2[20] ~= kFingerprintNumber * 0.3 * 0.8 = 2400
  EXPECT_THAT(id_counts_index_0,
              UnorderedElementsAre(Pair(10, 1986), Pair(20, 8014)));
  EXPECT_THAT(id_counts_index_1,
              UnorderedElementsAre(Pair(10, 570), Pair(20, 2377)));
  // Same pool chance: 0.2 * 0.2 + 0.8 * 0.8 = 0.68.
  // Expect value for samePool is around kFingerprintNumber * 0.3 * 0.68 = 2040
  // Different pool chance: 0.2 * 0.8 * 2 = 0.32.
  // Expect value for samePool is around kFingerprintNumber * 0.3 * 0.32 = 960
  EXPECT_EQ(same_pool, 2000);
  EXPECT_EQ(different_pool, 947);
}

TEST(BranchNodeImplTest, TestMultiplicityLessThanOne) {
  // The branch node has 2 branches.
  // One branch is selected with 20% chance, which is a population node always
  // assigns virtual person id 10.
  // The other branch is selected with 80% chance, which is a population node
  // always assigns virtual person id 20.
  // Multiplicity is explicit value = 0.3. Each event will have 0 or 1 virtual
  // person.
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestBranchNode"
        index: 1
        branch_node {
          branches {
            node {
              population_node {
                pools { population_offset: 10 total_population: 1 }
                random_seed: "TestPopulationNodeSeed1"
              }
            }
            chance: 0.2
          }
          branches {
            node {
              population_node {
                pools { population_offset: 20 total_population: 1 }
                random_seed: "TestPopulationNodeSeed2"
              }
            }
            chance: 0.8
          }
          random_seed: "TestBranchNodeSeed"
          multiplicity {
            expected_multiplicity: 0.3
            max_value: 2
            cap_at_max: true
            person_index_field: "multiplicity_person_index"
            random_seed: "test multiplicity"
          }
        }
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<ModelNode> node,
                       ModelNode::Build(config));

  absl::flat_hash_map<uint64_t, int32_t> id_counts;
  for (int fingerprint = 0; fingerprint < kFingerprintNumber; ++fingerprint) {
    LabelerEvent input;
    input.set_acting_fingerprint(fingerprint);
    EXPECT_THAT(node->Apply(input), IsOk());
    EXPECT_THAT(input.virtual_person_activities().size(), AnyOf(0, 1));
    if (input.virtual_person_activities().size() == 1) {
      ++id_counts[input.virtual_person_activities(0).virtual_person_id()];
    }
  }

  // Compare to the exact result to make sure C++ and Kotlin implementations
  // behave the same.
  // The expected count for 10 is kFingerprintNumber * 0.3 * 0.2 = 600
  // The expected count for 20 is kFingerprintNumber * 0.3 * 0.8 = 2400
  EXPECT_THAT(id_counts, UnorderedElementsAre(Pair(10, 593), Pair(20, 2354)));
}

TEST(BranchNodeImplTest, TestMultiplicityFromField) {
  // The branch node has 2 branches.
  // One branch is selected with 20% chance, which is a population node always
  // assigns virtual person id 10.
  // The other branch is selected with 80% chance, which is a population node
  // always assigns virtual person id 20.
  // Multiplicity is read from a field.
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestBranchNode"
        index: 1
        branch_node {
          branches {
            node {
              population_node {
                pools { population_offset: 10 total_population: 1 }
                random_seed: "TestPopulationNodeSeed1"
              }
            }
            chance: 0.2
          }
          branches {
            node {
              population_node {
                pools { population_offset: 20 total_population: 1 }
                random_seed: "TestPopulationNodeSeed2"
              }
            }
            chance: 0.8
          }
          random_seed: "TestBranchNodeSeed"
          multiplicity {
            expected_multiplicity_field: "expected_multiplicity"
            max_value: 2
            cap_at_max: true
            person_index_field: "multiplicity_person_index"
            random_seed: "test multiplicity"
          }
        }
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<ModelNode> node,
                       ModelNode::Build(config));

  absl::flat_hash_map<uint64_t, int32_t> id_counts_index_0;
  absl::flat_hash_map<uint64_t, int32_t> id_counts_index_1;
  int64_t same_pool = 0;
  int64_t different_pool = 0;
  for (int fingerprint = 0; fingerprint < kFingerprintNumber; ++fingerprint) {
    LabelerEvent input;
    input.set_acting_fingerprint(fingerprint);

    // Set multiplicity for this event.
    input.set_expected_multiplicity(1.3);

    EXPECT_THAT(node->Apply(input), IsOk());
    EXPECT_THAT(input.virtual_person_activities().size(), AnyOf(1, 2));
    ++id_counts_index_0[input.virtual_person_activities(0).virtual_person_id()];
    if (input.virtual_person_activities().size() == 2) {
      ++id_counts_index_1[input.virtual_person_activities(1)
                              .virtual_person_id()];
      if (input.virtual_person_activities(0).virtual_person_id() ==
          input.virtual_person_activities(1).virtual_person_id()) {
        ++same_pool;
      } else {
        ++different_pool;
      }
    }
  }

  // Expect ~70% events have 1 virtual person, ~30% events have 2.
  // The clones are labeled independently, so they have the same probability
  // to go to each pool.
  // Compare to the exact result to make sure C++ and Kotlin implementations
  // behave the same.
  // Expected values are
  //   id_counts_index_0[10] ~= kFingerprintNumber * 0.8 = 8000
  //   id_counts_index_0[20] ~= kFingerprintNumber * 0.2 = 2000
  //   id_counts_index_1[10] ~= kFingerprintNumber * 0.3 * 0.2 = 600
  //   id_counts_index_1[20] ~= kFingerprintNumber * 0.3 * 0.8 = 2400

  EXPECT_THAT(id_counts_index_0,
              UnorderedElementsAre(Pair(10, 1986), Pair(20, 8014)));
  EXPECT_THAT(id_counts_index_1,
              UnorderedElementsAre(Pair(10, 570), Pair(20, 2377)));
  // Same pool chance: 0.2 * 0.2 + 0.8 * 0.8 = 0.68.
  // Expect value for same_pool is around kFingerprintNumber * 0.3 * 0.68 = 2040
  // Different pool chance: 0.2 * 0.8 * 2 = 0.32.
  // Expect value for different_pool is around kFingerprintNumber * 0.3 * 0.32 =
  // 960
  EXPECT_EQ(2000, same_pool);
  EXPECT_EQ(947, different_pool);
}

TEST(BranchNodeImplTest, TestUsePersonIndex) {
  // The branch node has 2 branches.
  // The first one is a population node always assigns virtual person id 10.
  // The second one is a population node always assigns virtual person id 20.
  // Select the first one if person_index = 0, else select the second one.
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestBranchNode"
        index: 1
        branch_node {
          branches {
            node {
              population_node {
                pools { population_offset: 10 total_population: 1 }
                random_seed: "TestPopulationNodeSeed1"
              }
            }
            condition { name: "multiplicity_person_index" op: EQUAL value: "0" }
          }
          branches {
            node {
              population_node {
                pools { population_offset: 20 total_population: 1 }
                random_seed: "TestPopulationNodeSeed2"
              }
            }
            condition { op: TRUE }
          }
          multiplicity {
            expected_multiplicity_field: "expected_multiplicity"
            max_value: 2
            cap_at_max: true
            person_index_field: "multiplicity_person_index"
            random_seed: "test multiplicity"
          }
        }
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<ModelNode> node,
                       ModelNode::Build(config));

  // person_index = 0 get id 10
  // person_index = 1 get id 20
  absl::flat_hash_map<uint64_t, double> id_counts;
  for (int fingerprint = 0; fingerprint < kFingerprintNumber; ++fingerprint) {
    LabelerEvent input;
    input.set_acting_fingerprint(fingerprint);

    // Set multiplicity for this event.
    input.set_expected_multiplicity(1.3);

    EXPECT_THAT(node->Apply(input), IsOk());
    EXPECT_THAT(input.virtual_person_activities().size(), AnyOf(1, 2));
    EXPECT_EQ(input.virtual_person_activities(0).virtual_person_id(), 10);
    if (input.virtual_person_activities().size() == 2) {
      EXPECT_EQ(input.virtual_person_activities(1).virtual_person_id(), 20);
    }

    for (auto& person : input.virtual_person_activities()) {
      ++id_counts[person.virtual_person_id()];
    }
  }

  // All events have person_index 0.
  // ~30% events have person_index 1.
  // Compare to the exact result to make sure C++ and Kotlin implementations
  // behave the same.
  EXPECT_EQ(id_counts[10], kFingerprintNumber);
  EXPECT_EQ(id_counts[20], 2947);
}

TEST(BranchNodeImplTest, TestNestedMultiplicity) {
  // There are two levels of multiplicity, attached to the corresponding branch
  // nodes.
  //
  // Note that in this case they should use different
  // expected_multiplicity_field(if read from field) and person_index_field.
  // To avoid conflict, we use expected_multiplicity in this test.
  // But using another person_index_field requires a proto change, and we will
  // only do that when there is an actual use case. So this test uses the same
  // person_index_field because it doesn't depend on person_index.
  //
  // The first level is a branch node with one child, and multiplicity = 1.2.
  // The second level is a branch node with two children, and
  // multiplicity = 1.6.
  // The first child is a population node always assigns virtual person id 10.
  // The second child is a population node always assigns virtual person id 20.
  // Select the first one with chance 0.2, the second one with chance 0.8.
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "BranchLevel1"
        branch_node {
          multiplicity {
            expected_multiplicity: 1.2
            max_value: 2
            cap_at_max: true
            person_index_field: "multiplicity_person_index"
            random_seed: "test multiplicity 1"
          }
          branches {
            condition { op: TRUE }
            node {
              name: "BranchLevel2"
              branch_node {
                multiplicity {
                  expected_multiplicity: 1.6
                  max_value: 2
                  cap_at_max: true
                  person_index_field: "multiplicity_person_index"
                  random_seed: "test multiplicity 2"
                }
                branches {
                  node {
                    population_node {
                      pools { population_offset: 10 total_population: 1 }
                      random_seed: "TestPopulationNodeSeed1"
                    }
                  }
                  chance: 0.2
                }
                branches {
                  node {
                    population_node {
                      pools { population_offset: 20 total_population: 1 }
                      random_seed: "TestPopulationNodeSeed2"
                    }
                  }
                  chance: 0.8
                }
                random_seed: "TestBranchNodeSeed"
              }
            }
          }
        }
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<ModelNode> node,
                       ModelNode::Build(config));

  absl::flat_hash_map<uint64_t, int32_t> id_counts;
  absl::flat_hash_map<uint64_t, int32_t> virtual_person_size_counts;
  for (int fingerprint = 0; fingerprint < kFingerprintNumber; ++fingerprint) {
    LabelerEvent input;
    input.set_acting_fingerprint(fingerprint);
    EXPECT_THAT(node->Apply(input), IsOk());
    EXPECT_THAT(input.virtual_person_activities().size(), AnyOf(1, 2, 3, 4));
    ++virtual_person_size_counts[input.virtual_person_activities().size()];
    for (auto& person : input.virtual_person_activities()) {
      ++id_counts[person.virtual_person_id()];
    }
  }

  // Total multiplicity = 1.2 * 1.6 = 1.92, 20% with id 10, 80% with id 20.
  // There can be 1-4 virtual persons for each event.
  // Level 1: 80% chance 1 clone, 20% chance 2 clones
  // Level 2: 40% chance 1 clones, 60% chance 2 clones
  // Probability for 1-4 virtual persons:
  // 1 VP: first level 1 clone, second level 1 clone: 0.8 * 0.4 = 0.32
  // 2 VP: first level 1 clone, second level 2 clones;
  //       or first level 2 clones, second level 1 clone:
  //    0.8 * 0.6 + 0.2 * 0.4 * 0.4 = 0.512
  // 3 VP: first level 2 clones, second level 1 clone + 2 clones(or 2 + 1):
  //    0.2 * 0.4 * 0.6 + 0.2 * 0.6 * 0.4 = 0.096
  // 4 VP: first level 2 clones, second level 2 clones + 2 clones:
  //    0.2 * 0.6 * 0.6 = 0.072
  //
  // Compare to the exact result to make sure C++ and Kotlin implementations
  // behave the same.
  EXPECT_THAT(id_counts, UnorderedElementsAre(Pair(10, 3884), Pair(20, 15384)));
  EXPECT_THAT(virtual_person_size_counts,
              UnorderedElementsAre(Pair(1, 3207), Pair(2, 5083), Pair(3, 945),
                                   Pair(4, 765)));
}

TEST(BranchNodeImplTest, TestMultiplicityPass1FoldsBackPoolAssignments) {
  // The branch node has one ranked-population-node branch and uses multiplicity
  // to clone events. In pool-identity (pass-1) mode each clone emits a
  // PoolAssignment instead of a virtual person. Every clone's assignment must
  // be folded back onto the event, so the pass-1 pool-assignment total must
  // equal the full-mode virtual-person total (one of each per clone).
  // Regression test for ApplyMultiplicity previously dropping pool assignments
  // from clones.
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestBranchNode"
        index: 1
        branch_node {
          branches {
            node {
              ranked_population_node {
                pools { population_offset: 100 total_population: 500 }
                random_seed: "TestRankedSeed"
                ranked_size: 200
                unranked_mode: DISJOINT
              }
            }
            chance: 1.0
          }
          random_seed: "TestBranchNodeSeed"
          multiplicity {
            expected_multiplicity: 3
            max_value: 1.2
            cap_at_max: true
            person_index_field: "multiplicity_person_index"
            random_seed: "test multiplicity"
          }
        }
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<ModelNode> node,
                       ModelNode::Build(config));

  // clone_count depends only on acting_fingerprint, so it is identical across
  // both modes. Full mode yields one virtual person per clone; pass-1 mode must
  // yield one pool assignment per clone.
  int64_t full_mode_total = 0;
  int64_t pass1_total = 0;
  for (int fingerprint = 0; fingerprint < kFingerprintNumber; ++fingerprint) {
    LabelerEvent full_input;
    full_input.set_acting_fingerprint(fingerprint);
    EXPECT_THAT(node->Apply(full_input), IsOk());
    full_mode_total += full_input.virtual_person_activities().size();

    LabelerEvent pass1_input;
    pass1_input.set_acting_fingerprint(fingerprint);
    pass1_input.set_pool_identity_mode(true);
    EXPECT_THAT(node->Apply(pass1_input), IsOk());
    EXPECT_EQ(pass1_input.virtual_person_activities().size(), 0);
    pass1_total += pass1_input.pool_assignments().size();
  }

  EXPECT_EQ(full_mode_total, pass1_total);
  EXPECT_GT(pass1_total, kFingerprintNumber);
}

}  // namespace
}  // namespace wfa_virtual_people
