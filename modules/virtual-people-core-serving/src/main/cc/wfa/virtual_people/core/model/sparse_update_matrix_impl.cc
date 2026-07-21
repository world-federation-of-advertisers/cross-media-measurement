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

#include "wfa/virtual_people/core/model/sparse_update_matrix_impl.h"

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
    const google::protobuf::RepeatedPtrField<SparseUpdateMatrix::Column>&
        columns,
    const google::protobuf::FieldMask& hash_field_mask) {
  std::vector<const LabelerEvent*> events;
  for (const SparseUpdateMatrix::Column& column : columns) {
    events.push_back(&column.column_attrs());
  }
  return HashFieldMaskMatcher::Build(events, hash_field_mask);
}

// Converts each column to a FieldFilter, and builds a FieldFiltersMatcher
// with all the FieldFilters.
absl::StatusOr<std::unique_ptr<FieldFiltersMatcher>> BuildFieldFiltersMatcher(
    const google::protobuf::RepeatedPtrField<SparseUpdateMatrix::Column>&
        columns) {
  std::vector<std::unique_ptr<FieldFilter>> filters;
  for (const SparseUpdateMatrix::Column& column : columns) {
    ASSIGN_OR_RETURN(filters.emplace_back(),
                     FieldFilter::New(column.column_attrs()));

    if (!filters.back()) {
      return absl::InternalError("FieldFilter::New should never return NULL.");
    }
  }
  return FieldFiltersMatcher::Build(std::move(filters));
}

absl::StatusOr<std::unique_ptr<DistributedConsistentHashing>> BuildRowsHashing(
    const SparseUpdateMatrix::Column& column) {
  // Gets the probabilities distribution of the rows, and build the hashing.
  std::vector<DistributionChoice> distribution;
  for (int i = 0; i < column.probabilities_size(); i++) {
    distribution.push_back(
        DistributionChoice({i, static_cast<double>(column.probabilities(i))}));
  }
  return DistributedConsistentHashing::Build(std::move(distribution));
}

absl::StatusOr<std::unique_ptr<SparseUpdateMatrixImpl>>
SparseUpdateMatrixImpl::Build(const SparseUpdateMatrix& config) {
  if (config.columns_size() == 0) {
    return absl::InvalidArgumentError(absl::StrCat(
        "No column exists in SparseUpdateMatrix: ", config.DebugString()));
  }

  for (const SparseUpdateMatrix::Column& column : config.columns()) {
    if (!column.has_column_attrs()) {
      return absl::InvalidArgumentError(
          absl::StrCat("No column_attrs in the column in SparseUpdateMatrix: ",
                       column.DebugString()));
    }
    if (column.rows_size() == 0) {
      return absl::InvalidArgumentError(
          absl::StrCat("No row exists in the column in SparseUpdateMatrix: ",
                       column.DebugString()));
    }
    if (column.rows_size() != column.probabilities_size()) {
      return absl::InvalidArgumentError(absl::StrCat(
          "Rows and probabilities are not aligned in the column in "
          "SparseUpdateMatrix: ",
          column.DebugString()));
    }
  }

  std::unique_ptr<HashFieldMaskMatcher> hash_matcher = nullptr;
  std::unique_ptr<FieldFiltersMatcher> filters_matcher = nullptr;
  if (config.has_hash_field_mask()) {
    ASSIGN_OR_RETURN(
        hash_matcher,
        BuildHashFieldMaskMatcher(config.columns(), config.hash_field_mask()));
    if (!hash_matcher) {
      return absl::InternalError(
          "HashFieldMaskMatcher::Build should never return NULL.");
    }
  } else {
    ASSIGN_OR_RETURN(filters_matcher,
                     BuildFieldFiltersMatcher(config.columns()));
    if (!filters_matcher) {
      return absl::InternalError(
          "FieldFiltersMatcher::Build should never return NULL.");
    }
  }

  // Converts the probabilities distribution of each column to
  // DistributedConsistentHashing.
  std::vector<std::unique_ptr<DistributedConsistentHashing>> row_hashings;
  // Keeps the rows of each column.
  std::vector<std::vector<LabelerEvent>> rows;
  for (const SparseUpdateMatrix::Column& column : config.columns()) {
    row_hashings.emplace_back();
    ASSIGN_OR_RETURN(row_hashings.back(), BuildRowsHashing(column));

    if (!row_hashings.back()) {
      return absl::InternalError(
          "DistributedConsistentHashing::Build should never return NULL.");
    }

    // Gets the rows.
    rows.emplace_back(column.rows().begin(), column.rows().end());
  }

  PassThroughNonMatches pass_through_non_matches =
      config.pass_through_non_matches() ? PassThroughNonMatches::kYes
                                        : PassThroughNonMatches::kNo;

  return absl::make_unique<SparseUpdateMatrixImpl>(
      std::move(hash_matcher), std::move(filters_matcher),
      std::move(row_hashings), config.random_seed(), std::move(rows),
      pass_through_non_matches);
}

absl::Status SparseUpdateMatrixImpl::Update(LabelerEvent& event) const {
  ASSIGN_OR_RETURN(MatrixIndexes indexes,
                   SelectFromMatrix(hash_matcher_.get(), filters_matcher_.get(),
                                    row_hashings_, random_seed_, event));
  int column_index = indexes.column_index;
  int row_index = indexes.row_index;
  if (column_index == kNoMatchingIndex) {
    if (pass_through_non_matches_ == PassThroughNonMatches::kYes) {
      return absl::OkStatus();
    } else {
      return absl::InvalidArgumentError(
          absl::StrCat("No column matching for event: ", event.DebugString()));
    }
  }

  if (column_index < 0 || column_index >= rows_.size()) {
    return absl::InternalError("The returned column index is out of range.");
  }
  if (row_index < 0 || row_index >= rows_[column_index].size()) {
    return absl::InternalError("The returned row index is out of range.");
  }

  event.MergeFrom(rows_[column_index][row_index]);
  return absl::OkStatus();
}

}  // namespace wfa_virtual_people
