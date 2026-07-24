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

#ifndef SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_CONDITIONAL_MERGE_IMPL_H_
#define SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_CONDITIONAL_MERGE_IMPL_H_

#include <memory>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "wfa/virtual_people/common/model.pb.h"
#include "wfa/virtual_people/core/model/attributes_updater.h"
#include "wfa/virtual_people/core/model/utils/field_filters_matcher.h"

namespace wfa_virtual_people {

class ConditionalMergeImpl : public AttributesUpdaterInterface {
 public:
  // Always use AttributesUpdaterInterface::Build to get an
  // AttributesUpdaterInterface object. Users should
  // not call the factory method or the constructor of the derived classes
  // directly.
  //
  // Returns error status when any of the following happens:
  //   @config.nodes is empty.
  //   @config.nodes.condition is not set.
  //   @config.nodes.update is not set.
  //   Fails to build FieldFilter from any @config.nodes.condition.
  static absl::StatusOr<std::unique_ptr<ConditionalMergeImpl>> Build(
      const ConditionalMerge& config);

  enum class PassThroughNonMatches { kNo, kYes };

  explicit ConditionalMergeImpl(std::unique_ptr<FieldFiltersMatcher> matcher,
                                std::vector<LabelerEvent>&& updates,
                                PassThroughNonMatches pass_through_non_matches)
      : matcher_(std::move(matcher)),
        updates_(std::move(updates)),
        pass_through_non_matches_(pass_through_non_matches) {}

  ConditionalMergeImpl(const ConditionalMergeImpl&) = delete;
  ConditionalMergeImpl& operator=(const ConditionalMergeImpl&) = delete;

  // Updates @event with selected node.
  // The node is selected by matching @event with conditions through matcher_.
  // The update of selected node is merged into @event.
  //
  // Returns error status if no node matches @event, and
  // pass_through_non_matches_ is kNo.
  absl::Status Update(LabelerEvent& event) const override;

 private:
  // The matcher used to match input events to the conditions.
  std::unique_ptr<FieldFiltersMatcher> matcher_;
  // The selected update will be merged to the input event.
  std::vector<LabelerEvent> updates_;
  // When calling Update, if no condition matches, returns OkStatus if
  // pass_through_non_matches_ is kYes, otherwise returns error status.
  PassThroughNonMatches pass_through_non_matches_;
};

}  // namespace wfa_virtual_people

#endif  // SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_CONDITIONAL_MERGE_IMPL_H_
