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

#ifndef SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_UTILS_HASH_FIELD_MASK_MATCHER_H_
#define SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_UTILS_HASH_FIELD_MASK_MATCHER_H_

#include <memory>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "google/protobuf/field_mask.pb.h"
#include "wfa/virtual_people/common/model.pb.h"

namespace wfa_virtual_people {

// Selects the index of the hash that matches the hash of the input
// LabelerEvent.
class HashFieldMaskMatcher {
 public:
  // Always use Build to get a HashFieldMaskMatcher object. Users should not
  // call the constructor below directly.
  //
  // Returns error status if any of the following happens:
  // * @events is empty.
  // * Any entry in @events is null.
  // * @hash_field_mask.paths is empty.
  // * Multiple entries in @events have the same hash value.
  static absl::StatusOr<std::unique_ptr<HashFieldMaskMatcher>> Build(
      const std::vector<const LabelerEvent*>& events,
      const google::protobuf::FieldMask& hash_field_mask);

  // Never call the constructor directly.
  explicit HashFieldMaskMatcher(
      absl::flat_hash_map<uint64_t, int>&& hashes,
      const google::protobuf::FieldMask& hash_field_mask)
      : hashes_(std::move(hashes)), hash_field_mask_(hash_field_mask) {}

  HashFieldMaskMatcher(const HashFieldMaskMatcher&) = delete;
  HashFieldMaskMatcher& operator=(const HashFieldMaskMatcher&) = delete;

  // Returns the index of the matching hash value in @hashs_.
  // Returns @kNoMatchingIndex if no matching.
  //
  // The matching is performed on the hash of @event.
  int GetMatch(const LabelerEvent& event) const;

 private:
  // Map from hash values to indexes.
  absl::flat_hash_map<uint64_t, int> hashes_;
  google::protobuf::FieldMask hash_field_mask_;
};

}  // namespace wfa_virtual_people

#endif  // SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_UTILS_HASH_FIELD_MASK_MATCHER_H_
