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

#include "wfa/virtual_people/core/model/utils/hash_field_mask_matcher.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "google/protobuf/field_mask.pb.h"
#include "google/protobuf/util/field_mask_util.h"
#include "src/farmhash.h"
#include "wfa/virtual_people/common/model.pb.h"
#include "wfa/virtual_people/core/model/utils/constants.h"

namespace wfa_virtual_people {

// Get the hash of @event, only including the fields in @hash_field_mask.
uint64_t HashLabelerEvent(const LabelerEvent& event,
                          const google::protobuf::FieldMask& hash_field_mask) {
  LabelerEvent hash_field_event;
  google::protobuf::util::FieldMaskUtil::MergeMessageTo(
      event, hash_field_mask,
      google::protobuf::util::FieldMaskUtil::MergeOptions(), &hash_field_event);
  return util::Fingerprint64(hash_field_event.SerializePartialAsString());
}

absl::StatusOr<std::unique_ptr<HashFieldMaskMatcher>>
HashFieldMaskMatcher::Build(
    const std::vector<const LabelerEvent*>& events,
    const google::protobuf::FieldMask& hash_field_mask) {
  if (events.empty()) {
    return absl::InvalidArgumentError(
        "The events is empty when building HashFieldMaskMatcher.");
  }
  if (hash_field_mask.paths_size() == 0) {
    return absl::InvalidArgumentError(
        "The hash_field_mask is empty when building HashFieldMaskMatcher.");
  }

  absl::flat_hash_map<uint64_t, int> hashes;
  int index = 0;
  for (const LabelerEvent* event : events) {
    if (!event) {
      return absl::InvalidArgumentError(
          "An event is null when building HashFieldMaskMatcher.");
    }
    uint64_t hash = HashLabelerEvent(*event, hash_field_mask);
    auto [iterator, inserted] = hashes.insert_or_assign(hash, index);
    if (!inserted) {
      return absl::InvalidArgumentError(
          "Multiple events have the same hash when applying hash field mask.");
    }
    ++index;
  }

  return absl::make_unique<HashFieldMaskMatcher>(std::move(hashes),
                                                 hash_field_mask);
}

int HashFieldMaskMatcher::GetMatch(const LabelerEvent& event) const {
  uint64_t event_hash = HashLabelerEvent(event, hash_field_mask_);
  auto it = hashes_.find(event_hash);
  if (it == hashes_.end()) {
    return kNoMatchingIndex;
  }
  return it->second;
}

}  // namespace wfa_virtual_people
