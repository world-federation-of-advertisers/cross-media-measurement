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

#include "wfa/virtual_people/core/model/conditional_assignment_impl.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "common_cpp/macros/macros.h"
#include "google/protobuf/descriptor.h"
#include "wfa/virtual_people/common/field_filter/field_filter.h"
#include "wfa/virtual_people/common/field_filter/utils/field_util.h"
#include "wfa/virtual_people/common/model.pb.h"
#include "wfa/virtual_people/core/model/attributes_updater.h"

namespace wfa_virtual_people {

template <typename ValueType, EnableIfProtoValueType<ValueType> = true>
void Assign(
    LabelerEvent& event,
    const std::vector<const google::protobuf::FieldDescriptor*>& source,
    const std::vector<const google::protobuf::FieldDescriptor*>& target) {
  ProtoFieldValue<ValueType> field_value =
      GetValueFromProto<ValueType>(event, source);
  if (!field_value.is_set) {
    return;
  }
  SetValueToProto<ValueType>(event, target, field_value.value);
}

absl::StatusOr<std::function<void(
    LabelerEvent&, const std::vector<const google::protobuf::FieldDescriptor*>&,
    const std::vector<const google::protobuf::FieldDescriptor*>&)>>
GetAssignmentFunction(
    const google::protobuf::FieldDescriptor::CppType cpp_type) {
  switch (cpp_type) {
    case google::protobuf::FieldDescriptor::CppType::CPPTYPE_INT32:
      return Assign<int32_t>;
    case google::protobuf::FieldDescriptor::CppType::CPPTYPE_INT64:
      return Assign<int64_t>;
    case google::protobuf::FieldDescriptor::CppType::CPPTYPE_UINT32:
      return Assign<uint32_t>;
    case google::protobuf::FieldDescriptor::CppType::CPPTYPE_UINT64:
      return Assign<uint64_t>;
    case google::protobuf::FieldDescriptor::CppType::CPPTYPE_BOOL:
      return Assign<bool>;
    case google::protobuf::FieldDescriptor::CppType::CPPTYPE_ENUM:
      return Assign<const google::protobuf::EnumValueDescriptor*>;
    case google::protobuf::FieldDescriptor::CppType::CPPTYPE_STRING:
      return Assign<const std::string&>;
    default:
      return absl::InvalidArgumentError(
          "Unsupported field type for ConditionalAssignment.");
  }
}

absl::StatusOr<std::unique_ptr<ConditionalAssignmentImpl>>
ConditionalAssignmentImpl::Build(const ConditionalAssignment& config) {
  if (!config.has_condition()) {
    return absl::InvalidArgumentError(
        absl::StrCat("Condition is not set in ConditionalAssignment: ",
                     config.DebugString()));
  }
  if (config.assignments_size() == 0) {
    return absl::InvalidArgumentError(absl::StrCat(
        "No assignments in ConditionalAssignment: ", config.DebugString()));
  }

  ASSIGN_OR_RETURN(
      std::unique_ptr<FieldFilter> condition,
      FieldFilter::New(LabelerEvent().GetDescriptor(), config.condition()));

  if (!condition) {
    return absl::InternalError("FieldFilter::New should never return NULL.");
  }

  std::vector<ConditionalAssignmentImpl::Assignment> assignments;

  for (const ConditionalAssignment::Assignment& assignment_config :
       config.assignments()) {
    if (!assignment_config.has_source_field()) {
      return absl::InvalidArgumentError(
          absl::StrCat("All assignments must have source_field set in "
                       "ConditionalAssignment: ",
                       config.DebugString()));
    }
    if (!assignment_config.has_target_field()) {
      return absl::InvalidArgumentError(
          absl::StrCat("All assignments must have target_field set in "
                       "ConditionalAssignment: ",
                       config.DebugString()));
    }

    ConditionalAssignmentImpl::Assignment& assignment =
        assignments.emplace_back();

    ASSIGN_OR_RETURN(assignment.source,
                     GetFieldFromProto(LabelerEvent().GetDescriptor(),
                                       assignment_config.source_field()));
    ASSIGN_OR_RETURN(assignment.target,
                     GetFieldFromProto(LabelerEvent().GetDescriptor(),
                                       assignment_config.target_field()));
    if (assignment.source.back()->cpp_type() !=
        assignment.target.back()->cpp_type()) {
      return absl::InvalidArgumentError(absl::StrCat(
          "All assignments must have source_field and target_field being the "
          "same type in ConditionalAssignment: ",
          config.DebugString()));
    }

    ASSIGN_OR_RETURN(
        assignment.assign,
        GetAssignmentFunction(assignment.source.back()->cpp_type()));
  }

  return absl::make_unique<ConditionalAssignmentImpl>(std::move(condition),
                                                      std::move(assignments));
}

absl::Status ConditionalAssignmentImpl::Update(LabelerEvent& event) const {
  if (condition_->IsMatch(event)) {
    for (const ConditionalAssignmentImpl::Assignment& assignment :
         assignments_) {
      assignment.assign(event, assignment.source, assignment.target);
    }
  }
  return absl::OkStatus();
}

}  // namespace wfa_virtual_people
