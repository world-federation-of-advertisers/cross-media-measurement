// Copyright 2023 The Cross-Media Measurement Authors
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

#ifndef SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_SELECTOR_VID_MODEL_SELECTOR_H_
#define SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_SELECTOR_VID_MODEL_SELECTOR_H_

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "google/type/date.pb.h"
#include "wfa/measurement/api/v2alpha/model_line.pb.h"
#include "wfa/measurement/api/v2alpha/model_rollout.pb.h"
#include "wfa/virtual_people/common/event.pb.h"
#include "wfa/virtual_people/core/selector/lru_cache.h"

namespace wfa_virtual_people {

using ::wfa::measurement::api::v2alpha::ModelLine;
using ::wfa::measurement::api::v2alpha::ModelRollout;

class VidModelSelector {
 public:
  // Factory method to create an instance of `VidModelSelector`.
  //
  // Returns an error if model_line name is unspecified or invalid and if
  // model_rollout is parented by a different model_line.
  static absl::StatusOr<VidModelSelector> Build(
      const ModelLine& model_line,
      const std::vector<ModelRollout>& model_rollouts);

  absl::StatusOr<std::optional<std::string>> GetModelRelease(
      const LabelerInput& labeler_input) const;

  // Move constructor
  VidModelSelector(VidModelSelector&& other) noexcept;

  // Move assignment operator
  VidModelSelector& operator=(VidModelSelector&& other) = delete;

 private:
  const ModelLine model_line_;
  const std::vector<ModelRollout> model_rollouts_;
  mutable LruCache lru_cache_;
  mutable std::mutex mtx_;

  // Class constructor. Private.
  // Instances of this class must be built using the factory method `Build`.
  explicit VidModelSelector(const ModelLine& model_line,
                            const std::vector<ModelRollout>& model_rollouts);

  // Access to the cache is synchronized to prevent multiple threads calculating
  // percentages in case of cache miss.
  std::vector<ModelReleasePercentile> ReadFromCache(
      const absl::CivilDay& event_date_utc) const;

  // Return a list of ModelReleasePercentile(s). Each ModelReleasePercentile
  // wraps the percentage of adoption of a particular ModelRelease and the
  // ModelRelease itself. The list is sorted by either rollout_period_start_date
  // or instant_rollout_date.
  //
  // The adoption percentage of each ModelRollout is calculated as follows:
  // (EVENT_DAY - ROLLOUT_START_DAY) / (ROLLOUT_END_DAY - ROLLOUT_START_DAY).
  //
  // In case a ModelRollout has the `rollout_freeze_date` set and the event day
  // is greater than rollout_freeze_date, the EVENT_DAY in the above formula is
  // replaced by `rollout_freeze_date` to ensure that the rollout stops its
  // expansion: (ROLLOUT_FREEZE_DATE - ROLLOUT_START_DAY) / (ROLLOUT_END_DAY -
  // ROLLOUT_START_DAY).
  //
  // In case of an instant rollout ROLLOUT_START_DATE is equal to
  // ROLLOUT_END_DATE.
  std::vector<ModelReleasePercentile> CalculatePercentages(
      const absl::CivilDay& event_date_utc) const;

  // Returns the percentage of events that this ModelRollout must label for the
  // given `event_date_utc`.
  double CalculatePercentageAdoption(const absl::CivilDay& event_date_utc,
                                     const ModelRollout& model_rollout) const;

  // Iterates through all available ModelRollout(s) sorted by either
  // `rollout_period_start_date` or `instant_rollout_date` from the most recent
  // to the oldest. The function keeps adding ModelRollout(s) to the
  // `active_rollouts` vector until the following condition is met:
  // event_date_utc
  // >= rollout_period_end_date && !rollout.has_rollout_freeze_date()
  std::vector<ModelRollout> RetrieveActiveRollouts(
      const absl::CivilDay& event_date_utc) const;
  absl::StatusOr<std::string> GetEventId(
      const LabelerInput& labeler_input) const;

  // Comparator used to sort a std::vector of `ModelRollout`.
  //
  // If `ModelRollout` has gradual rollout period use the `start_date`.
  // Otherwise use the `instant_rollout_date`.
  bool CompareModelRollouts(const ModelRollout& lhs,
                            const ModelRollout& rhs) const;
};

}  // namespace wfa_virtual_people

#endif  // SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_SELECTOR_VID_MODEL_SELECTOR_H_
