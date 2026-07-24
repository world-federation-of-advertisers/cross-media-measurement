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

#ifndef SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_UPDATE_MATRIX_IMPL_H_
#define SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_UPDATE_MATRIX_IMPL_H_

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

class UpdateMatrixImpl : public AttributesUpdaterInterface {
 public:
  // Always use AttributesUpdaterInterface::Build to get an
  // AttributesUpdaterInterface object. Users should
  // not call the factory method or the constructor of the derived classes
  // directly.
  //
  // Returns error status when any of the following happens:
  //   @config.rows is empty.
  //   @config.columns is empty.
  //   In @config, the probabilities count does not equal to rows count
  //     multiplies columns count.
  //   Fails to build FieldFilter from any column.
  //   Fails to build DistributedConsistentHashing from the probabilities
  //     distribution of any column.
  static absl::StatusOr<std::unique_ptr<UpdateMatrixImpl>> Build(
      const UpdateMatrix& config);

  enum class PassThroughNonMatches { kNo, kYes };

  explicit UpdateMatrixImpl(
      std::unique_ptr<HashFieldMaskMatcher> hash_matcher,
      std::unique_ptr<FieldFiltersMatcher> filters_matcher,
      std::vector<std::unique_ptr<DistributedConsistentHashing>>&& row_hashings,
      absl::string_view random_seed, std::vector<LabelerEvent>&& rows,
      PassThroughNonMatches pass_through_non_matches)
      : hash_matcher_(std::move(hash_matcher)),
        filters_matcher_(std::move(filters_matcher)),
        row_hashings_(std::move(row_hashings)),
        random_seed_(random_seed),
        rows_(std::move(rows)),
        pass_through_non_matches_(pass_through_non_matches) {}

  UpdateMatrixImpl(const UpdateMatrixImpl&) = delete;
  UpdateMatrixImpl& operator=(const UpdateMatrixImpl&) = delete;

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
  // The seed used in hashing.
  std::string random_seed_;
  // All the rows, of which the selected row will be merged to the input event.
  std::vector<LabelerEvent> rows_;
  // When calling Update, if no column matches, returns OkStatus if
  // pass_through_non_matches_ is kYes, otherwise returns error status.
  PassThroughNonMatches pass_through_non_matches_;
};

}  // namespace wfa_virtual_people

#endif  // SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_UPDATE_MATRIX_IMPL_H_
