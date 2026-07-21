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

#include "wfa/virtual_people/core/model/utils/update_matrix_helper.h"

#include <memory>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "wfa/virtual_people/common/model.pb.h"
#include "wfa/virtual_people/core/model/utils/constants.h"
#include "wfa/virtual_people/core/model/utils/distributed_consistent_hashing.h"
#include "wfa/virtual_people/core/model/utils/field_filters_matcher.h"
#include "wfa/virtual_people/core/model/utils/hash_field_mask_matcher.h"

namespace wfa_virtual_people {

absl::StatusOr<MatrixIndexes> SelectFromMatrix(
    const HashFieldMaskMatcher* hash_matcher,
    const FieldFiltersMatcher* filters_matcher,
    const std::vector<std::unique_ptr<DistributedConsistentHashing>>&
        row_hashings,
    absl::string_view random_seed, const LabelerEvent& event) {
  int column_index = kNoMatchingIndex;
  if (hash_matcher) {
    column_index = hash_matcher->GetMatch(event);
  } else if (filters_matcher) {
    column_index = filters_matcher->GetFirstMatch(event);
  } else {
    return absl::InternalError("No column matcher is set.");
  }
  if (column_index == kNoMatchingIndex) {
    return MatrixIndexes({kNoMatchingIndex, kNoMatchingIndex});
  }
  if (column_index < 0 || column_index >= row_hashings.size()) {
    return absl::InternalError("The returned index is out of range.");
  }
  int row_index = row_hashings[column_index]->Hash(
      absl::StrCat(random_seed, event.acting_fingerprint()));
  return MatrixIndexes({column_index, row_index});
}

}  // namespace wfa_virtual_people
