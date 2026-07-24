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

#ifndef SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_CONDITIONAL_ASSIGNMENT_IMPL_H_
#define SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_CONDITIONAL_ASSIGNMENT_IMPL_H_

#include <memory>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "google/protobuf/descriptor.h"
#include "wfa/virtual_people/common/field_filter/field_filter.h"
#include "wfa/virtual_people/common/model.pb.h"
#include "wfa/virtual_people/core/model/attributes_updater.h"

namespace wfa_virtual_people {

class ConditionalAssignmentImpl : public AttributesUpdaterInterface {
 public:
  // Always use AttributesUpdaterInterface::Build to get an
  // AttributesUpdaterInterface object. Users should
  // not call the factory method or the constructor of the derived classes
  // directly.
  //
  // Returns error status when any of the following happens:
  // * @config.condition is not set.
  // * @config.assignments is empty.
  // * Fails to build a FieldFilter from @config.condition.
  // * In any entry of @config.assignments, source_field or target_field is not
  //   set or does not refer to a valid field.
  // * In any entry of @config.assignments, source_field and target_field refer
  //   to different type of fields. (like int32 vs int64)
  static absl::StatusOr<std::unique_ptr<ConditionalAssignmentImpl>> Build(
      const ConditionalAssignment& config);

  struct Assignment {
    std::vector<const google::protobuf::FieldDescriptor*> source;
    std::vector<const google::protobuf::FieldDescriptor*> target;
    std::function<void(
        LabelerEvent&,
        const std::vector<const google::protobuf::FieldDescriptor*>&,
        const std::vector<const google::protobuf::FieldDescriptor*>&)>
        assign;
  };

  explicit ConditionalAssignmentImpl(std::unique_ptr<FieldFilter> condition,
                                     std::vector<Assignment>&& assignments)
      : condition_(std::move(condition)),
        assignments_(std::move(assignments)) {}

  ConditionalAssignmentImpl(const ConditionalAssignmentImpl&) = delete;
  ConditionalAssignmentImpl& operator=(const ConditionalAssignmentImpl&) =
      delete;

  // If condition_ is matched, for each entry in assignments_, assigns the
  // value of source field to target field.
  // If condition_ is not matched, does nothing and returns OK status.
  absl::Status Update(LabelerEvent& event) const override;

 private:
  // Applies the assignments if condition_ is matched.
  std::unique_ptr<FieldFilter> condition_;
  // Each entry in assignments_ contains a source field and a target field.
  std::vector<Assignment> assignments_;
};

}  // namespace wfa_virtual_people

#endif  // SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_CONDITIONAL_ASSIGNMENT_IMPL_H_
