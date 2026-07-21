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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "common_cpp/macros/macros.h"
#include "google/protobuf/text_format.h"
#include "src/farmhash.h"
#include "wfa/virtual_people/common/event.pb.h"
#include "wfa/virtual_people/common/label.pb.h"
#include "wfa/virtual_people/common/model.pb.h"
#include "wfa/virtual_people/core/model/model_node.h"

namespace wfa_virtual_people {

absl::StatusOr<std::unique_ptr<Labeler>> Labeler::Build(
    const CompiledNode& root) {
  ASSIGN_OR_RETURN(std::unique_ptr<ModelNode> root_node,
                   ModelNode::Build(root));
  return absl::make_unique<Labeler>(std::move(root_node));
}

absl::StatusOr<std::unique_ptr<Labeler>> Labeler::Build(
    const std::vector<CompiledNode>& nodes) {
  std::unique_ptr<ModelNode> root = nullptr;
  absl::flat_hash_map<uint32_t, std::unique_ptr<ModelNode>> node_refs;

  for (const CompiledNode& node_config : nodes) {
    if (root) {
      return absl::InvalidArgumentError(
          "No node is allowed after the root node.");
    }
    if (node_config.has_index()) {
      ASSIGN_OR_RETURN(std::unique_ptr<ModelNode> node,
                       ModelNode::Build(node_config, node_refs));
      if (!node_refs.insert({node_config.index(), std::move(node)}).second) {
        return absl::InvalidArgumentError(
            absl::StrCat("Duplicated indexes: ", node_config.index()));
      }
    } else {
      ASSIGN_OR_RETURN(root, ModelNode::Build(node_config, node_refs));
    }
  }

  if (!root) {
    if (node_refs.empty()) {
      // This should never happen.
      return absl::InternalError("Cannot find root node.");
    }
    // We expect only 1 node in the node_refs map, which is the root node.
    root = std::move(node_refs.extract(node_refs.begin()).mapped());
  }

  if (!root) {
    // This should never happen.
    return absl::InternalError("Root is NULL.");
  }

  if (!node_refs.empty()) {
    return absl::InvalidArgumentError("Some nodes are not in the model tree.");
  }

  return absl::make_unique<Labeler>(std::move(root));
}

void SetUserInfoFingerprint(UserInfo& user_info) {
  if (user_info.has_user_id()) {
    user_info.set_user_id_fingerprint(util::Fingerprint64(user_info.user_id()));
  }
}

// Generates fingerprints for event_id and user_id.
// The default value of acting_fingerprint is the fingerprint of event_id.
void SetFingerprints(LabelerEvent& event) {
  LabelerInput* labeler_input = event.mutable_labeler_input();

  if (labeler_input->has_event_id()) {
    uint64_t event_id_fingerprint =
        util::Fingerprint64(labeler_input->event_id().id());
    labeler_input->mutable_event_id()->set_id_fingerprint(event_id_fingerprint);
    event.set_acting_fingerprint(event_id_fingerprint);
  }

  if (!labeler_input->has_profile_info()) {
    return;
  }

  ProfileInfo* profile_info = labeler_input->mutable_profile_info();
  if (profile_info->has_email_user_info()) {
    SetUserInfoFingerprint(*profile_info->mutable_email_user_info());
  }
  if (profile_info->has_phone_user_info()) {
    SetUserInfoFingerprint(*profile_info->mutable_phone_user_info());
  }
  if (profile_info->has_logged_in_id_user_info()) {
    SetUserInfoFingerprint(*profile_info->mutable_logged_in_id_user_info());
  }
  if (profile_info->has_logged_out_id_user_info()) {
    SetUserInfoFingerprint(*profile_info->mutable_logged_out_id_user_info());
  }
  if (profile_info->has_proprietary_id_space_1_user_info()) {
    SetUserInfoFingerprint(
        *profile_info->mutable_proprietary_id_space_1_user_info());
  }
  if (profile_info->has_proprietary_id_space_2_user_info()) {
    SetUserInfoFingerprint(
        *profile_info->mutable_proprietary_id_space_2_user_info());
  }
  if (profile_info->has_proprietary_id_space_3_user_info()) {
    SetUserInfoFingerprint(
        *profile_info->mutable_proprietary_id_space_3_user_info());
  }
  if (profile_info->has_proprietary_id_space_4_user_info()) {
    SetUserInfoFingerprint(
        *profile_info->mutable_proprietary_id_space_4_user_info());
  }
  if (profile_info->has_proprietary_id_space_5_user_info()) {
    SetUserInfoFingerprint(
        *profile_info->mutable_proprietary_id_space_5_user_info());
  }
  if (profile_info->has_proprietary_id_space_6_user_info()) {
    SetUserInfoFingerprint(
        *profile_info->mutable_proprietary_id_space_6_user_info());
  }
  if (profile_info->has_proprietary_id_space_7_user_info()) {
    SetUserInfoFingerprint(
        *profile_info->mutable_proprietary_id_space_7_user_info());
  }
  if (profile_info->has_proprietary_id_space_8_user_info()) {
    SetUserInfoFingerprint(
        *profile_info->mutable_proprietary_id_space_8_user_info());
  }
  if (profile_info->has_proprietary_id_space_9_user_info()) {
    SetUserInfoFingerprint(
        *profile_info->mutable_proprietary_id_space_9_user_info());
  }
  if (profile_info->has_proprietary_id_space_10_user_info()) {
    SetUserInfoFingerprint(
        *profile_info->mutable_proprietary_id_space_10_user_info());
  }
}

absl::Status Labeler::Label(const LabelerInput& input,
                            LabelerOutput& output) const {
  return Label(input, output, LabelingMode::kFull);
}

absl::Status Labeler::Label(const LabelerInput& input, LabelerOutput& output,
                            LabelingMode mode) const {
  // Prepare labeler event.
  LabelerEvent event;
  *event.mutable_labeler_input() = input;
  SetFingerprints(event);

  if (mode == LabelingMode::kPoolIdentity) {
    event.set_pool_identity_mode(true);
  }

  // Apply model.
  RETURN_IF_ERROR(root_->Apply(event));

  // Populate data to output.
  *output.mutable_people() = event.virtual_person_activities();

  // Copy pool assignments from event to output (populated in pass-1 mode).
  *output.mutable_pool_assignments() = event.pool_assignments();

  if (input.enable_debug_trace()) {
    // TODO(@tcsnfkx): Update the content of debug trace. Currently only set the
    // debug trace to be the LabelerEvent. Use TextFormat::PrintToString rather
    // than DebugString(): newer protobuf intentionally makes DebugString()
    // emit a non-roundtrippable redaction marker.
    std::string trace;
    google::protobuf::TextFormat::PrintToString(event, &trace);
    output.set_serialized_debug_trace(trace);
  }
  return absl::OkStatus();
}

}  // namespace wfa_virtual_people
