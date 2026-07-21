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

#ifndef SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_UTILS_FIELD_FILTERS_MATCHER_H_
#define SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_UTILS_FIELD_FILTERS_MATCHER_H_

#include <memory>
#include <utility>
#include <vector>

#include "absl/status/statusor.h"
#include "wfa/virtual_people/common/field_filter.pb.h"
#include "wfa/virtual_people/common/field_filter/field_filter.h"
#include "wfa/virtual_people/common/model.pb.h"

namespace wfa_virtual_people {

// Selects the field filter that a LabelerEvent matches.
class FieldFiltersMatcher {
 public:
  // Always use Build to get a FieldFiltersMatcher object. Users should not call
  // the constructor below directly.
  //
  // Returns error status if any of the following happens:
  //   @filter_configs is empty.
  //   Any FieldFilterProto in @filter_configs is invalid.
  static absl::StatusOr<std::unique_ptr<FieldFiltersMatcher>> Build(
      const std::vector<const FieldFilterProto*>& filter_configs);

  static absl::StatusOr<std::unique_ptr<FieldFiltersMatcher>> Build(
      std::vector<std::unique_ptr<FieldFilter>>&& filters);

  // Never call the constructor directly.
  explicit FieldFiltersMatcher(
      std::vector<std::unique_ptr<FieldFilter>>&& filters)
      : filters_(std::move(filters)) {}

  FieldFiltersMatcher(const FieldFiltersMatcher&) = delete;
  FieldFiltersMatcher& operator=(const FieldFiltersMatcher&) = delete;

  // Returns the index of the first matching FieldFilter in @filters_.
  // Returns @kNoMatchingIndex if no matching.
  //
  // The matching is performed on @event.
  int GetFirstMatch(const LabelerEvent& event) const;

 private:
  std::vector<std::unique_ptr<FieldFilter>> filters_;
};

}  // namespace wfa_virtual_people

#endif  // SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_UTILS_FIELD_FILTERS_MATCHER_H_
