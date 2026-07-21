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
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "common_cpp/testing/status_macros.h"
#include "common_cpp/testing/status_matchers.h"
#include "gmock/gmock.h"
#include "google/protobuf/text_format.h"
#include "gtest/gtest.h"
#include "wfa/virtual_people/common/model.pb.h"
#include "wfa/virtual_people/core/model/attributes_updater.h"
#include "wfa/virtual_people/core/model/model_node.h"

namespace wfa_virtual_people {
namespace {

using ::wfa::IsOk;
using ::wfa::StatusIs;

TEST(UpdateTreeImplTest, TestEmptyTree) {
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        update_tree { root { stop_node {} } }
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<AttributesUpdaterInterface> updater,
                       AttributesUpdaterInterface::Build(config));

  LabelerEvent event;
  EXPECT_THAT(updater->Update(event), IsOk());
}

TEST(UpdateTreeImplTest, TestSingleBranch) {
  // An UpdateTree that updates person_country_code from "COUNTRY_1" to
  // "UPDATED_COUNTRY_1". If person_country_code is not "COUNTRY_1", return
  // error status.
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        update_tree {
          root {
            branch_node {
              branches {
                node { stop_node {} }
                condition { op: TRUE }
              }
              updates {
                updates {
                  update_matrix {
                    columns { person_country_code: "COUNTRY_1" }
                    rows { person_country_code: "UPDATED_COUNTRY_1" }
                    probabilities: 1
                  }
                }
              }
            }
          }
        }
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<AttributesUpdaterInterface> updater,
                       AttributesUpdaterInterface::Build(config));

  LabelerEvent event_1;
  EXPECT_THAT(updater->Update(event_1),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));

  LabelerEvent event_2;
  event_2.set_person_country_code("COUNTRY_2");
  EXPECT_THAT(updater->Update(event_2),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));

  LabelerEvent event_3;
  event_3.set_person_country_code("COUNTRY_1");
  EXPECT_THAT(updater->Update(event_3), IsOk());
  EXPECT_EQ(event_3.person_country_code(), "UPDATED_COUNTRY_1");
}

TEST(UpdateTreeImplTest, TestTwoBranches) {
  // An UpdateTree that updates person_country_code from "COUNTRY_1" to
  // "UPDATED_COUNTRY_1", and from "COUNTRY_2" to "UPDATED_COUNTRY_2". If
  // person_country_code is not "COUNTRY_1" or "COUNTRY_2", return error status.
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        update_tree {
          root {
            branch_node {
              branches {
                node {
                  branch_node {
                    branches {
                      node { stop_node {} }
                      condition { op: TRUE }
                    }
                    updates {
                      updates {
                        update_matrix {
                          columns { person_country_code: "COUNTRY_1" }
                          rows { person_country_code: "UPDATED_COUNTRY_1" }
                          probabilities: 1
                        }
                      }
                    }
                  }
                }
                condition {
                  name: "person_country_code"
                  op: EQUAL
                  value: "COUNTRY_1"
                }
              }
              branches {
                node {
                  branch_node {
                    branches {
                      node { stop_node {} }
                      condition { op: TRUE }
                    }
                    updates {
                      updates {
                        update_matrix {
                          columns { person_country_code: "COUNTRY_2" }
                          rows { person_country_code: "UPDATED_COUNTRY_2" }
                          probabilities: 1
                        }
                      }
                    }
                  }
                }
                condition {
                  name: "person_country_code"
                  op: EQUAL
                  value: "COUNTRY_2"
                }
              }
            }
          }
        }
      )pb",
      &config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<AttributesUpdaterInterface> updater,
                       AttributesUpdaterInterface::Build(config));

  LabelerEvent event_1;
  EXPECT_THAT(updater->Update(event_1),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));

  LabelerEvent event_2;
  event_2.set_person_country_code("COUNTRY_3");
  EXPECT_THAT(updater->Update(event_2),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));

  LabelerEvent event_3;
  event_3.set_person_country_code("COUNTRY_1");
  EXPECT_THAT(updater->Update(event_3), IsOk());
  EXPECT_EQ(event_3.person_country_code(), "UPDATED_COUNTRY_1");

  LabelerEvent event_4;
  event_4.set_person_country_code("COUNTRY_2");
  EXPECT_THAT(updater->Update(event_4), IsOk());
  EXPECT_EQ(event_4.person_country_code(), "UPDATED_COUNTRY_2");
}

TEST(UpdateTreeImplTest, TestTwoBranchesWithIndexes) {
  // An UpdateTree that updates person_country_code from "COUNTRY_1" to
  // "UPDATED_COUNTRY_1", and from "COUNTRY_2" to "UPDATED_COUNTRY_2". If
  // person_country_code is not "COUNTRY_1" or "COUNTRY_2", return error status.
  BranchNode::AttributesUpdater config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        update_tree {
          root {
            branch_node {
              branches {
                node_index: 1
                condition {
                  name: "person_country_code"
                  op: EQUAL
                  value: "COUNTRY_1"
                }
              }
              branches {
                node_index: 2
                condition {
                  name: "person_country_code"
                  op: EQUAL
                  value: "COUNTRY_2"
                }
              }
            }
          }
        }
      )pb",
      &config));

  CompiledNode node_config_1;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        index: 1
        branch_node {
          branches {
            node { stop_node {} }
            condition { op: TRUE }
          }
          updates {
            updates {
              update_matrix {
                columns { person_country_code: "COUNTRY_1" }
                rows { person_country_code: "UPDATED_COUNTRY_1" }
                probabilities: 1
              }
            }
          }
        }
      )pb",
      &node_config_1));

  CompiledNode node_config_2;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        index: 2
        branch_node {
          branches {
            node { stop_node {} }
            condition { op: TRUE }
          }
          updates {
            updates {
              update_matrix {
                columns { person_country_code: "COUNTRY_2" }
                rows { person_country_code: "UPDATED_COUNTRY_2" }
                probabilities: 1
              }
            }
          }
        }
      )pb",
      &node_config_2));

  absl::flat_hash_map<uint32_t, std::unique_ptr<ModelNode>> node_refs;
  ASSERT_OK_AND_ASSIGN(node_refs[1], ModelNode::Build(node_config_1));
  ASSERT_OK_AND_ASSIGN(node_refs[2], ModelNode::Build(node_config_2));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<AttributesUpdaterInterface> updater,
                       AttributesUpdaterInterface::Build(config, node_refs));

  LabelerEvent event_1;
  EXPECT_THAT(updater->Update(event_1),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));

  LabelerEvent event_2;
  event_2.set_person_country_code("COUNTRY_3");
  EXPECT_THAT(updater->Update(event_2),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));

  LabelerEvent event_3;
  event_3.set_person_country_code("COUNTRY_1");
  EXPECT_THAT(updater->Update(event_3), IsOk());
  EXPECT_EQ(event_3.person_country_code(), "UPDATED_COUNTRY_1");

  LabelerEvent event_4;
  event_4.set_person_country_code("COUNTRY_2");
  EXPECT_THAT(updater->Update(event_4), IsOk());
  EXPECT_EQ(event_4.person_country_code(), "UPDATED_COUNTRY_2");
}

}  // namespace
}  // namespace wfa_virtual_people
