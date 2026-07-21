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

#ifndef SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_UTILS_UPDATE_MATRIX_HELPER_H_
#define SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_UTILS_UPDATE_MATRIX_HELPER_H_

#include <memory>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "wfa/virtual_people/common/model.pb.h"
#include "wfa/virtual_people/core/model/utils/distributed_consistent_hashing.h"
#include "wfa/virtual_people/core/model/utils/field_filters_matcher.h"
#include "wfa/virtual_people/core/model/utils/hash_field_mask_matcher.h"

namespace wfa_virtual_people {

struct MatrixIndexes {
  int column_index;
  int row_index;
};

// Gets the indexes of the selected column and row.
// Column is selected by applying @hash_matcher or @filters_matcher on the
// @event.
// Row is selected by applying the hashing of the selected column from
// @row_hashings.
//
// When no column is selected, returns {kNoMatchingIndex, kNoMatchingIndex}.
//
// @random_seed is used as part of the seed when selecting row by hashing.
//
// At least one of @hash_matcher and @filters_matcher cannot be null.
// The output of applying @hash_matcher or @filters_matcher must be in the range
// of [0, @row_hashings.size() - 1].
absl::StatusOr<MatrixIndexes> SelectFromMatrix(
    const HashFieldMaskMatcher* hash_matcher,
    const FieldFiltersMatcher* filters_matcher,
    const std::vector<std::unique_ptr<DistributedConsistentHashing>>&
        row_hashings,
    absl::string_view random_seed, const LabelerEvent& event);

}  // namespace wfa_virtual_people

#endif  // SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_UTILS_UPDATE_MATRIX_HELPER_H_
