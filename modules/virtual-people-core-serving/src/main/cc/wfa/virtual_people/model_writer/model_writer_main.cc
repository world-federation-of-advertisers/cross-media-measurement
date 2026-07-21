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

// This tool takes a model in single node representation, converts to
// node list representation, then write to file in Riegeli format.
// Example usage:
// bazel run //src/main/cc/wfa/virtual_people/model_writer:model_writer_main \
// -- \
// --input_model_path=/tmp/model_writer/single_node_model.txt \
// --output_model_path=/tmp/model_writer/node_list_model_riegeli

#include <filesystem>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "common_cpp/protobuf_util/riegeli_io.h"
#include "glog/logging.h"
#include "google/protobuf/io/zero_copy_stream_impl.h"
#include "google/protobuf/text_format.h"
#include "wfa/virtual_people/common/model.pb.h"
#include "wfa/virtual_people/core/model/model_serializer.h"

ABSL_FLAG(std::string, input_model_path, "",
          "Path to the input model file. The file contains textproto of a "
          "CompiledNode. This represents the root node of the model tree, "
          "and all nodes in the model tree are defined as CompiledNode "
          "directly, not using node index.");
ABSL_FLAG(std::string, output_model_path, "",
          "Path to the output model file. The model is converted to node "
          "list representation, and child nodes are referenced by indexes. "
          "The model is written in Riegeli format.");

namespace {

wfa_virtual_people::CompiledNode ReadSingleNodeModel(absl::string_view path) {
  int fd = open(path.data(), O_RDONLY);
  CHECK(fd > 0) << "Unable to open file: " << path;
  google::protobuf::io::FileInputStream file_input(fd);
  file_input.SetCloseOnDelete(true);
  wfa_virtual_people::CompiledNode root;
  CHECK(google::protobuf::TextFormat::Parse(&file_input, &root))
      << "Unable to parse textproto file: " << path;
  return root;
}

}  // namespace

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);
  google::InitGoogleLogging(argv[0]);

  CHECK(!absl::GetFlag(FLAGS_input_model_path).empty())
      << "Must set input_model_path";
  CHECK(!absl::GetFlag(FLAGS_output_model_path).empty())
      << "Must set output_model_path";

  wfa_virtual_people::CompiledNode root =
      ReadSingleNodeModel(absl::GetFlag(FLAGS_input_model_path));

  absl::StatusOr<std::vector<wfa_virtual_people::CompiledNode>> node_list =
      wfa_virtual_people::ToNodeListRepresentation(root);
  CHECK(node_list.ok()) << "Failed to convert to node list representation: "
                        << node_list.status();

  absl::Status write_status =
      wfa::WriteRiegeliFile<wfa_virtual_people::CompiledNode>(
          absl::GetFlag(FLAGS_output_model_path), *node_list);
  CHECK(write_status.ok()) << "Failed to write to file." << write_status;

  std::cout << "Model written to " << absl::GetFlag(FLAGS_output_model_path)
            << std::endl;

  return 0;
}
