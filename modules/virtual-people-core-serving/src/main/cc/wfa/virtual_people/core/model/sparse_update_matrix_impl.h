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

#ifndef SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_SPARSE_UPDATE_MATRIX_IMPL_H_
#define SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_SPARSE_UPDATE_MATRIX_IMPL_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "wfa/virtual_people/common/model.pb.h"
#include "wfa/virtual_people/core/model/attributes_updater.h"
#include "wfa/virtual_people/core/model/utils/distributed_consistent_hashing.h"
#include "wfa/virtual_people/core/model/utils/field_filters_matcher.h"
#include "wfa/virtual_people/core/model/utils/hash_field_mask_matcher.h"

namespace wfa_virtual_people {

// A representation of update matrix, which only contains the entries that the
// probabilities are not zero.
// Example:
// The following sparse update matrix
//     columns {
//       column_attrs { person_country_code: "COUNTRY_1" }
//       rows { person_country_code: "UPDATED_COUNTRY_1" }
//       rows { person_country_code: "UPDATED_COUNTRY_2" }
//       probabilities: 0.8
//       probabilities: 0.2
//     }
//     columns {
//       column_attrs { person_country_code: "COUNTRY_2" }
//       rows { person_country_code: "UPDATED_COUNTRY_1" }
//       rows { person_country_code: "UPDATED_COUNTRY_2" }
//       rows { person_country_code: "UPDATED_COUNTRY_3" }
//       probabilities: 0.2
//       probabilities: 0.4
//       probabilities: 0.4
//     }
//     columns {
//       column_attrs { person_country_code: "COUNTRY_3" }
//       rows { person_country_code: "UPDATED_COUNTRY_3" }
//       probabilities: 1.0
//     }
//     pass_through_non_matches: false
//     random_seed: "TestSeed"
// represents the matrix
//                          "COUNTRY_1"  "COUNTRY_2"  "COUNTRY_3"
//     "UPDATED_COUNTRY_1"      0.8          0.2            0
//     "UPDATED_COUNTRY_2"      0.2          0.4            0
//     "UPDATED_COUNTRY_3"        0          0.4          1.0
// The column is selected by the matched person_country_code, and the row is
// selected by probabilities of the selected column.
class SparseUpdateMatrixImpl : public AttributesUpdaterInterface {
 public:
  // Always use AttributesUpdaterInterface::Build to get an
  // AttributesUpdaterInterface object. Users should
  // not call the factory method or the constructor of the derived classes
  // directly.
  //
  // Returns error status when any of the following happens:
  //   @config.columns is empty.
  //   @config.columns.column_attrs is not set.
  //   @config.columns.rows is empty.
  //   In any @config.columns, the counts of probabilities and rows are not
  //     equal.
  //   Fails to build FieldFilter from any @config.columns.column_attrs.
  //   Fails to build DistributedConsistentHashing from the probabilities
  //     distribution of any @config.columns.
  static absl::StatusOr<std::unique_ptr<SparseUpdateMatrixImpl>> Build(
      const SparseUpdateMatrix& config);

  enum class PassThroughNonMatches { kNo, kYes };

  explicit SparseUpdateMatrixImpl(
      std::unique_ptr<HashFieldMaskMatcher> hash_matcher,
      std::unique_ptr<FieldFiltersMatcher> filters_matcher,
      std::vector<std::unique_ptr<DistributedConsistentHashing>>&& row_hashings,
      absl::string_view random_seed,
      std::vector<std::vector<LabelerEvent>>&& rows,
      PassThroughNonMatches pass_through_non_matches)
      : hash_matcher_(std::move(hash_matcher)),
        filters_matcher_(std::move(filters_matcher)),
        row_hashings_(std::move(row_hashings)),
        random_seed_(random_seed),
        rows_(std::move(rows)),
        pass_through_non_matches_(pass_through_non_matches) {}

  SparseUpdateMatrixImpl(const SparseUpdateMatrixImpl&) = delete;
  SparseUpdateMatrixImpl& operator=(const SparseUpdateMatrixImpl&) = delete;

  // Updates @event with selected row.
  // The row is selected in 2 steps
  // 1. Select the column with @event matches the condition.
  // 2. Use hashing to select the row based on the probabilities distribution of
  //    the column.
  //
  // Returns error status if no column matches @event, and
  // pass_through_non_matches_ is kNo.
  absl::Status Update(LabelerEvent& event) const override;

 private:
  // The matcher used to match input events to the column events when using hash
  // field mask.
  std::unique_ptr<HashFieldMaskMatcher> hash_matcher_;
  // The matcher used to match input events to the column conditions when not
  // using hash field mask.
  std::unique_ptr<FieldFiltersMatcher> filters_matcher_;
  // Each entry of the vector represents a hashing based on the probability
  // distribution of a column.
  // The size of the vector is the columns count.
  std::vector<std::unique_ptr<DistributedConsistentHashing>> row_hashings_;
  // The seed used in hashing during row selection after a column is matched.
  std::string random_seed_;
  // Each entry of the vector contains all the rows of the corresponding column.
  // The selected row will be merged to the input event.
  std::vector<std::vector<LabelerEvent>> rows_;
  // When calling Update, if no column matches, returns OkStatus if
  // pass_through_non_matches_ is kYes, otherwise returns error status.
  PassThroughNonMatches pass_through_non_matches_;
};

}  // namespace wfa_virtual_people

#endif  // SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_SPARSE_UPDATE_MATRIX_IMPL_H_
