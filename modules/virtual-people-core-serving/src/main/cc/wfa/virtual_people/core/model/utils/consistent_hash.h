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

#ifndef SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_UTILS_CONSISTENT_HASH_H_
#define SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_UTILS_CONSISTENT_HASH_H_

#include <cstdint>

namespace wfa_virtual_people {

// Applies consistent hashing, to map the input key to one of the buckets.
// Each bucket is represented by an index with range [0, num_buckets - 1].
// The output is the index of the selected bucket.
//
// The consistent hashing algorithm is from the published paper:
//   https://arxiv.org/pdf/1406.2294.pdf
int32_t JumpConsistentHash(uint64_t key, int32_t num_buckets);

}  // namespace wfa_virtual_people

#endif  // SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_UTILS_CONSISTENT_HASH_H_
