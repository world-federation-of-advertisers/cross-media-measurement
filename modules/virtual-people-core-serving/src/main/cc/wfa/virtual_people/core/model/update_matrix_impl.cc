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

#include "wfa/virtual_people/core/model/update_matrix_impl.h"

#include <memory>
#include <utility>
#include <vector>

#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "common_cpp/macros/macros.h"
#include "google/protobuf/field_mask.pb.h"
#include "google/protobuf/repeated_field.h"
#include "wfa/virtual_people/common/field_filter/field_filter.h"
#include "wfa/virtual_people/common/model.pb.h"
#include "wfa/virtual_people/core/model/attributes_updater.h"
#include "wfa/virtual_people/core/model/utils/constants.h"
#include "wfa/virtual_people/core/model/utils/update_matrix_helper.h"

namespace wfa_virtual_people {

absl::StatusOr<std::unique_ptr<HashFieldMaskMatcher>> BuildHashFieldMaskMatcher(
    const google::protobuf::RepeatedPtrField<LabelerEvent>& columns,
    const google::protobuf::FieldMask& hash_field_mask) {
  std::vector<const LabelerEvent*> events;
  for (const LabelerEvent& column : columns) {
    events.push_back(&column);
  }
  return HashFieldMaskMatcher::Build(events, hash_field_mask);
}

// Converts each column to a FieldFilter, and builds a FieldFiltersMatcher
// with all the FieldFilters.
absl::StatusOr<std::unique_ptr<FieldFiltersMatcher>> BuildFieldFiltersMatcher(
    const google::protobuf::RepeatedPtrField<LabelerEvent>& columns) {
  std::vector<std::unique_ptr<FieldFilter>> filters;
  for (const LabelerEvent& column : columns) {
    filters.emplace_back();
    ASSIGN_OR_RETURN(filters.back(), FieldFilter::New(column));
  }
  return FieldFiltersMatcher::Build(std::move(filters));
}

absl::StatusOr<std::unique_ptr<UpdateMatrixImpl>> UpdateMatrixImpl::Build(
    const UpdateMatrix& config) {
  int row_count = config.rows_size();
  int column_count = config.columns_size();
  if (row_count == 0) {
    return absl::InvalidArgumentError(
        absl::StrCat("No row exists in UpdateMatrix: ", config.DebugString()));
  }
  if (column_count == 0) {
    return absl::InvalidArgumentError(absl::StrCat(
        "No column exists in UpdateMatrix: ", config.DebugString()));
  }
  if (row_count * column_count != config.probabilities_size()) {
    return absl::InvalidArgumentError(
        absl::StrCat("Probabilities count must equal to row * column: ",
                     config.DebugString()));
  }

  std::unique_ptr<HashFieldMaskMatcher> hash_matcher = nullptr;
  std::unique_ptr<FieldFiltersMatcher> filters_matcher = nullptr;
  if (config.has_hash_field_mask()) {
    ASSIGN_OR_RETURN(
        hash_matcher,
        BuildHashFieldMaskMatcher(config.columns(), config.hash_field_mask()));
  } else {
    ASSIGN_OR_RETURN(filters_matcher,
                     BuildFieldFiltersMatcher(config.columns()));
  }

  // Converts the probabilities distribution of each column to
  // DistributedConsistentHashing.
  std::vector<std::unique_ptr<DistributedConsistentHashing>> row_hashings;
  for (int column_index = 0; column_index < column_count; ++column_index) {
    // Gets the probabilities distribution of the column, and build the hashing.
    std::vector<DistributionChoice> distribution;
    for (int row_index = 0; row_index < row_count; ++row_index) {
      float probability =
          config.probabilities(row_index * column_count + column_index);
      distribution.push_back(
          DistributionChoice({row_index, static_cast<double>(probability)}));
    }
    row_hashings.emplace_back();
    ASSIGN_OR_RETURN(row_hashings.back(), DistributedConsistentHashing::Build(
                                              std::move(distribution)));
  }

  std::vector<LabelerEvent> rows = {config.rows().begin(), config.rows().end()};

  PassThroughNonMatches pass_through_non_matches =
      config.pass_through_non_matches() ? PassThroughNonMatches::kYes
                                        : PassThroughNonMatches::kNo;

  return absl::make_unique<UpdateMatrixImpl>(
      std::move(hash_matcher), std::move(filters_matcher),
      std::move(row_hashings), config.random_seed(), std::move(rows),
      pass_through_non_matches);
}

absl::Status UpdateMatrixImpl::Update(LabelerEvent& event) const {
  ASSIGN_OR_RETURN(MatrixIndexes indexes,
                   SelectFromMatrix(hash_matcher_.get(), filters_matcher_.get(),
                                    row_hashings_, random_seed_, event));

  if (indexes.column_index == kNoMatchingIndex) {
    if (pass_through_non_matches_ == PassThroughNonMatches::kYes) {
      return absl::OkStatus();
    } else {
      return absl::InvalidArgumentError(
          absl::StrCat("No column matching for event: ", event.DebugString()));
    }
  }

  int row_index = indexes.row_index;
  if (row_index < 0 || row_index >= rows_.size()) {
    return absl::InternalError("The returned row index is out of range.");
  }

  event.MergeFrom(rows_[row_index]);
  return absl::OkStatus();
}

}  // namespace wfa_virtual_people
