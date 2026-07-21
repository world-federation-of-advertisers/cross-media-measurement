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

#include "absl/container/flat_hash_map.h"
#include "common_cpp/testing/common_matchers.h"
#include "common_cpp/testing/status_macros.h"
#include "common_cpp/testing/status_matchers.h"
#include "gmock/gmock.h"
#include "google/protobuf/text_format.h"
#include "gtest/gtest.h"
#include "wfa/virtual_people/common/label.pb.h"
#include "wfa/virtual_people/common/model.pb.h"
#include "wfa/virtual_people/core/model/model_node.h"

namespace wfa_virtual_people {
namespace {

using ::testing::DoubleNear;
using ::testing::Ge;
using ::testing::Lt;
using ::testing::Pair;
using ::testing::UnorderedElementsAre;
using ::wfa::EqualsProto;
using ::wfa::IsOk;
using ::wfa::StatusIs;

constexpr int kFingerprintNumber = 10000;

TEST(PopulationNodeImplTest, Apply) {
  CompiledNode config;
  // Test for a pool at the border of the reserved range.
  // 999999999999999997 = 10^18 - 3
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestPopulationNode"
        index: 1
        population_node {
          pools { population_offset: 10 total_population: 3 }
          pools { population_offset: 999999999999999997 total_population: 3 }
          pools { population_offset: 20 total_population: 4 }
          random_seed: "TestRandomSeed"
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
  // behave the same. The result should be around 0.1 * kFingerprintNumber =
  // 1000
  EXPECT_THAT(id_counts,
              UnorderedElementsAre(
                  Pair(10, 1076), Pair(11, 990), Pair(12, 975),
                  Pair(999999999999999997, 985), Pair(999999999999999998, 982),
                  Pair(999999999999999999, 981), Pair(20, 991), Pair(21, 1010),
                  Pair(22, 1005), Pair(23, 1005)));
}

TEST(PopulationNodeImplTest, ApplyNoLabel) {
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestPopulationNode"
        index: 1
        population_node {
          pools { population_offset: 10 total_population: 1 }
          random_seed: "TestRandomSeed"
        }
      )pb",
      &config));

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<ModelNode> node,
                       ModelNode::Build(config));

  LabelerEvent input;
  input.set_acting_fingerprint(kFingerprintNumber);
  EXPECT_THAT(node->Apply(input), IsOk());

  LabelerEvent expected_event;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        virtual_person_activities { virtual_person_id: 10 }
        acting_fingerprint: 10000
      )pb",
      &expected_event));
  EXPECT_THAT(input, EqualsProto(expected_event));
}

TEST(PopulationNodeImplTest, ApplyWithLabel) {
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestPopulationNode"
        index: 1
        population_node {
          pools { population_offset: 10 total_population: 1 }
          random_seed: "TestRandomSeed"
        }
      )pb",
      &config));

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<ModelNode> node,
                       ModelNode::Build(config));

  LabelerEvent input;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        label {
          demo {
            gender: GENDER_FEMALE
            age { min_age: 25 max_age: 1000 }
          }
        }
        acting_fingerprint: 10000
      )pb",
      &input));
  EXPECT_THAT(node->Apply(input), IsOk());
  LabelerEvent expected_event;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        virtual_person_activities {
          virtual_person_id: 10
          label {
            demo {
              gender: GENDER_FEMALE
              age { min_age: 25 max_age: 1000 }
            }
          }
        }
        label {
          demo {
            gender: GENDER_FEMALE
            age { min_age: 25 max_age: 1000 }
          }
        }
        acting_fingerprint: 10000
      )pb",
      &expected_event));
  EXPECT_THAT(input, EqualsProto(expected_event));
}

TEST(PopulationNodeImplTest, ApplyWithSingleQuantumLabel) {
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestPopulationNode"
        index: 1
        population_node {
          pools { population_offset: 10 total_population: 1 }
          random_seed: "TestRandomSeed"
        }
      )pb",
      &config));

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<ModelNode> node,
                       ModelNode::Build(config));

  LabelerEvent input;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        quantum_labels {
          quantum_labels {
            labels {
              demo {
                gender: GENDER_FEMALE
                age { min_age: 25 max_age: 1000 }
              }
            }
            labels {
              demo {
                gender: GENDER_MALE
                age { min_age: 25 max_age: 1000 }
              }
            }
            probabilities: 1.0
            probabilities: 0.0
            seed: "CollapseSeed"
          }
        }
        acting_fingerprint: 10000
      )pb",
      &input));
  EXPECT_THAT(node->Apply(input), IsOk());
  LabelerEvent expected_event;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        virtual_person_activities {
          virtual_person_id: 10
          label {
            demo {
              gender: GENDER_FEMALE
              age { min_age: 25 max_age: 1000 }
            }
          }
        }
        quantum_labels {
          quantum_labels {
            labels {
              demo {
                gender: GENDER_FEMALE
                age { min_age: 25 max_age: 1000 }
              }
            }
            labels {
              demo {
                gender: GENDER_MALE
                age { min_age: 25 max_age: 1000 }
              }
            }
            probabilities: 1.0
            probabilities: 0.0
            seed: "CollapseSeed"
          }
        }
        acting_fingerprint: 10000
      )pb",
      &expected_event));
  EXPECT_THAT(input, EqualsProto(expected_event));
}

TEST(PopulationNodeImplTest, ApplyWithMultipleQuantumLabels) {
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestPopulationNode"
        index: 1
        population_node {
          pools { population_offset: 10 total_population: 1 }
          random_seed: "TestRandomSeed"
        }
      )pb",
      &config));

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<ModelNode> node,
                       ModelNode::Build(config));

  LabelerEvent input;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        quantum_labels {
          quantum_labels {
            labels { demo { gender: GENDER_FEMALE } }
            labels { demo { gender: GENDER_MALE } }
            probabilities: 1.0
            probabilities: 0.0
            seed: "CollapseSeed"
          }
          quantum_labels {
            labels { demo { age { min_age: 25 max_age: 1000 } } }
            labels { demo { age { min_age: 1 max_age: 24 } } }
            probabilities: 1.0
            probabilities: 0.0
            seed: "CollapseSeed"
          }
        }
        acting_fingerprint: 10000
      )pb",
      &input));
  EXPECT_THAT(node->Apply(input), IsOk());
  LabelerEvent expected_event;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        virtual_person_activities {
          virtual_person_id: 10
          label {
            demo {
              gender: GENDER_FEMALE
              age { min_age: 25 max_age: 1000 }
            }
          }
        }
        quantum_labels {
          quantum_labels {
            labels { demo { gender: GENDER_FEMALE } }
            labels { demo { gender: GENDER_MALE } }
            probabilities: 1.0
            probabilities: 0.0
            seed: "CollapseSeed"
          }
          quantum_labels {
            labels { demo { age { min_age: 25 max_age: 1000 } } }
            labels { demo { age { min_age: 1 max_age: 24 } } }
            probabilities: 1.0
            probabilities: 0.0
            seed: "CollapseSeed"
          }
        }
        acting_fingerprint: 10000
      )pb",
      &expected_event));
  EXPECT_THAT(input, EqualsProto(expected_event));
}

TEST(PopulationNodeImplTest, ApplyWithMultipleQuantumLabelsOverride) {
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestPopulationNode"
        index: 1
        population_node {
          pools { population_offset: 10 total_population: 1 }
          random_seed: "TestRandomSeed"
        }
      )pb",
      &config));

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<ModelNode> node,
                       ModelNode::Build(config));

  // The collapsed quantum label can override existing collapsed quantum labels.
  LabelerEvent input;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        quantum_labels {
          quantum_labels {
            labels { demo { gender: GENDER_FEMALE } }
            labels { demo { gender: GENDER_MALE } }
            probabilities: 1.0
            probabilities: 0.0
            seed: "CollapseSeed"
          }
          quantum_labels {
            labels { demo { gender: GENDER_FEMALE } }
            labels { demo { gender: GENDER_MALE } }
            probabilities: 0.0
            probabilities: 1.0
            seed: "CollapseSeed"
          }
        }
        acting_fingerprint: 10000
      )pb",
      &input));
  EXPECT_THAT(node->Apply(input), IsOk());
  LabelerEvent expected_event;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        virtual_person_activities {
          virtual_person_id: 10
          label { demo { gender: GENDER_MALE } }
        }
        quantum_labels {
          quantum_labels {
            labels { demo { gender: GENDER_FEMALE } }
            labels { demo { gender: GENDER_MALE } }
            probabilities: 1.0
            probabilities: 0.0
            seed: "CollapseSeed"
          }
          quantum_labels {
            labels { demo { gender: GENDER_FEMALE } }
            labels { demo { gender: GENDER_MALE } }
            probabilities: 0.0
            probabilities: 1.0
            seed: "CollapseSeed"
          }
        }
        acting_fingerprint: 10000
      )pb",
      &expected_event));
  EXPECT_THAT(input, EqualsProto(expected_event));
}

TEST(PopulationNodeImplTest, ApplyWithQuantumLabelAndClassicLabel) {
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestPopulationNode"
        index: 1
        population_node {
          pools { population_offset: 10 total_population: 1 }
          random_seed: "TestRandomSeed"
        }
      )pb",
      &config));

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<ModelNode> node,
                       ModelNode::Build(config));

  LabelerEvent input;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        quantum_labels {
          quantum_labels {
            labels { demo { gender: GENDER_FEMALE } }
            labels { demo { gender: GENDER_MALE } }
            probabilities: 1.0
            probabilities: 0.0
            seed: "CollapseSeed"
          }
        }
        label { demo { age { min_age: 25 max_age: 1000 } } }
        acting_fingerprint: 10000
      )pb",
      &input));
  EXPECT_THAT(node->Apply(input), IsOk());
  LabelerEvent expected_event;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        virtual_person_activities {
          virtual_person_id: 10
          label {
            demo {
              gender: GENDER_FEMALE
              age { min_age: 25 max_age: 1000 }
            }
          }
        }
        quantum_labels {
          quantum_labels {
            labels { demo { gender: GENDER_FEMALE } }
            labels { demo { gender: GENDER_MALE } }
            probabilities: 1.0
            probabilities: 0.0
            seed: "CollapseSeed"
          }
        }
        label { demo { age { min_age: 25 max_age: 1000 } } }
        acting_fingerprint: 10000
      )pb",
      &expected_event));
  EXPECT_THAT(input, EqualsProto(expected_event));
}

TEST(PopulationNodeImplTest, ApplyWithQuantumLabelAndClassicLabelOverride) {
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestPopulationNode"
        index: 1
        population_node {
          pools { population_offset: 10 total_population: 1 }
          random_seed: "TestRandomSeed"
        }
      )pb",
      &config));

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<ModelNode> node,
                       ModelNode::Build(config));

  // The classic label can override existing collapsed quantum labels.
  LabelerEvent input;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        quantum_labels {
          quantum_labels {
            labels { demo { gender: GENDER_FEMALE } }
            labels { demo { gender: GENDER_MALE } }
            probabilities: 1.0
            probabilities: 0.0
            seed: "CollapseSeed"
          }
        }
        label { demo { gender: GENDER_MALE } }
        acting_fingerprint: 10000
      )pb",
      &input));
  EXPECT_THAT(node->Apply(input), IsOk());
  LabelerEvent expected_event;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        virtual_person_activities {
          virtual_person_id: 10
          label { demo { gender: GENDER_MALE } }
        }
        quantum_labels {
          quantum_labels {
            labels { demo { gender: GENDER_FEMALE } }
            labels { demo { gender: GENDER_MALE } }
            probabilities: 1.0
            probabilities: 0.0
            seed: "CollapseSeed"
          }
        }
        label { demo { gender: GENDER_MALE } }
        acting_fingerprint: 10000
      )pb",
      &expected_event));
  EXPECT_THAT(input, EqualsProto(expected_event));
}

TEST(PopulationNodeImplTest, ApplyExistingVirtualPerson) {
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestPopulationNode"
        index: 1
        population_node {
          pools { population_offset: 10 total_population: 1 }
          random_seed: "TestRandomSeed"
        }
      )pb",
      &config));

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<ModelNode> node,
                       ModelNode::Build(config));

  LabelerEvent input;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        virtual_person_activities { virtual_person_id: 10 }
        acting_fingerprint: 10000
      )pb",
      &input));

  EXPECT_THAT(node->Apply(input),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(PopulationNodeImplTest, CookieMonsterPool) {
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestPopulationNode"
        index: 1
        population_node {
          pools {
            population_offset: 1000000000000000000
            total_population: 100000000000000
          }
          random_seed: "TestRandomSeed"
        }
      )pb",
      &config));

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<ModelNode> node,
                       ModelNode::Build(config));

  LabelerEvent input;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        label {
          demo {
            gender: GENDER_FEMALE
            age { min_age: 25 max_age: 1000 }
          }
        }
        acting_fingerprint: 10000
      )pb",
      &input));
  EXPECT_THAT(node->Apply(input), IsOk());
  EXPECT_THAT(input.virtual_person_activities(0).virtual_person_id(),
              Ge(1000000000000000000));
  EXPECT_THAT(input.virtual_person_activities(0).virtual_person_id(),
              Lt(1000000000000000000 + 100000000000000));
}

TEST(PopulationNodeImplTest, InvalidEmptyPool) {
  // The node is invalid as the total pools size is 0.
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestPopulationNode"
        index: 1
        population_node {
          pools { population_offset: 10 total_population: 0 }
          pools { population_offset: 30 total_population: 0 }
          pools { population_offset: 20 total_population: 0 }
          random_seed: "TestRandomSeed"
        }
      )pb",
      &config));

  EXPECT_THAT(ModelNode::Build(config).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(PopulationNodeImplTest, InvalidPoolUsingReservedIdRange) {
  // The node is invalid as it uses the reserved id range.
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestPopulationNode"
        index: 1
        population_node {
          pools { population_offset: 0 total_population: 1000000000000000001 }
          random_seed: "TestRandomSeed"
        }
      )pb",
      &config));

  EXPECT_THAT(ModelNode::Build(config).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

}  // namespace
}  // namespace wfa_virtual_people
