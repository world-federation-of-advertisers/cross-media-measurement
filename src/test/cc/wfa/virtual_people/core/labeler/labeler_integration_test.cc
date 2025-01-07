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

#include <memory>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "common_cpp/protobuf_util/riegeli_io.h"
#include "common_cpp/protobuf_util/textproto_io.h"
#include "common_cpp/testing/common_matchers.h"
#include "common_cpp/testing/status_macros.h"
#include "common_cpp/testing/status_matchers.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "wfa/virtual_people/common/event.pb.h"
#include "wfa/virtual_people/common/label.pb.h"
#include "wfa/virtual_people/common/model.pb.h"
#include "wfa/virtual_people/core/labeler/labeler.h"

namespace wfa_virtual_people {
namespace {

using ::wfa::EqualsProto;
using ::wfa::IsOk;
using ::wfa::ReadRiegeliFile;
using ::wfa::ReadTextProtoFile;

const char kTestDataDir[] = "src/main/resources/testing/labeler/";

void ApplyAndValidate(absl::string_view model_path,
                      absl::string_view input_path,
                      absl::string_view output_path, bool is_single_node_file) {
  SCOPED_TRACE(absl::StrCat("ApplyAndValidate(", model_path, ", ", input_path,
                            ", ", output_path));
  std::unique_ptr<Labeler> labeler = nullptr;
  if (is_single_node_file) {
    CompiledNode root;
    EXPECT_THAT(ReadTextProtoFile(absl::StrCat(kTestDataDir, model_path), root),
                IsOk());
    ASSERT_OK_AND_ASSIGN(labeler, Labeler::Build(root));
  } else {
    std::vector<CompiledNode> nodes;
    EXPECT_THAT(ReadRiegeliFile(absl::StrCat(kTestDataDir, model_path), nodes),
                IsOk());
    ASSERT_OK_AND_ASSIGN(labeler, Labeler::Build(nodes));
  }

  LabelerInput input;
  EXPECT_THAT(ReadTextProtoFile(absl::StrCat(kTestDataDir, input_path), input),
              IsOk());

  LabelerOutput output;
  EXPECT_THAT(labeler->Label(input, output), IsOk());
  output.clear_serialized_debug_trace();

  LabelerOutput expected_output;
  EXPECT_THAT(ReadTextProtoFile(absl::StrCat(kTestDataDir, output_path),
                                expected_output),
              IsOk());
  EXPECT_THAT(output, EqualsProto(expected_output));
}

TEST(LabelerIntegrationTest, TestBuildFromRoot) {
  std::string single_node_model_path = "toy_model.textproto";
  std::string node_list_model_path = "toy_model_riegeli_list";
  absl::flat_hash_map<std::string, std::string> input_output_paths;
  for (int i = 1; i < 19; ++i) {
    input_output_paths[absl::StrFormat("labeler_input_%02d.textproto", i)] =
        absl::StrFormat("labeler_output_%02d.textproto", i);
  }

  for (auto &[input_path, output_path] : input_output_paths) {
    ApplyAndValidate(single_node_model_path, input_path, output_path,
                     /* is_single_node_file = */ true);
    ApplyAndValidate(node_list_model_path, input_path, output_path,
                     /* is_single_node_file = */ false);
  }
}

TEST(LabelerIntegrationTest, TestSingleIdModel) {
  std::string single_node_model_path = "single_id_model.textproto";
  std::string node_list_model_path = "single_id_model_riegeli_list";
  absl::flat_hash_map<std::string, std::string> input_output_paths;
  for (int i = 1; i < 19; ++i) {
    input_output_paths[absl::StrFormat("labeler_input_%02d.textproto", i)] =
        "single_id_labeler_output.textproto";
  }

  for (auto &[input_path, output_path] : input_output_paths) {
    ApplyAndValidate(single_node_model_path, input_path, output_path,
                     /* is_single_node_file = */ true);
    ApplyAndValidate(node_list_model_path, input_path, output_path,
                     /* is_single_node_file = */ false);
  }
}

}  // namespace
}  // namespace wfa_virtual_people
