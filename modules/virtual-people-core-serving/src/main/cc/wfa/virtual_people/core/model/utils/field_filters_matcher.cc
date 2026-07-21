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

#include "wfa/virtual_people/core/model/utils/field_filters_matcher.h"

#include <memory>
#include <utility>
#include <vector>

#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "common_cpp/macros/macros.h"
#include "google/protobuf/message.h"
#include "wfa/virtual_people/common/field_filter.pb.h"
#include "wfa/virtual_people/common/field_filter/field_filter.h"
#include "wfa/virtual_people/common/model.pb.h"
#include "wfa/virtual_people/core/model/utils/constants.h"

namespace wfa_virtual_people {

absl::StatusOr<std::unique_ptr<FieldFiltersMatcher>> FieldFiltersMatcher::Build(
    const std::vector<const FieldFilterProto*>& filter_configs) {
  if (filter_configs.empty()) {
    return absl::InvalidArgumentError(
        "The given FieldFilterProto configs is empty.");
  }

  // Converts each FieldFilterProto to FieldFilter. Breaks if encounters any
  // error status.
  std::vector<std::unique_ptr<FieldFilter>> filters;
  for (const FieldFilterProto* filter_config : filter_configs) {
    filters.emplace_back();
    ASSIGN_OR_RETURN(
        filters.back(),
        FieldFilter::New(LabelerEvent().GetDescriptor(), *filter_config));
  }

  return absl::make_unique<FieldFiltersMatcher>(std::move(filters));
}

absl::StatusOr<std::unique_ptr<FieldFiltersMatcher>> FieldFiltersMatcher::Build(
    std::vector<std::unique_ptr<FieldFilter>>&& filters) {
  return absl::make_unique<FieldFiltersMatcher>(std::move(filters));
}

int FieldFiltersMatcher::GetFirstMatch(const LabelerEvent& event) const {
  int index = 0;
  for (auto& filter : filters_) {
    if (filter->IsMatch(event)) {
      return index;
    }
    ++index;
  }
  return kNoMatchingIndex;
}

}  // namespace wfa_virtual_people
