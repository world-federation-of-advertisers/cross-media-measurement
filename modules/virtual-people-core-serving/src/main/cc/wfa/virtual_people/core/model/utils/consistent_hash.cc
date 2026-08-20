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

#include "wfa/virtual_people/core/model/utils/consistent_hash.h"

#include <cstdint>

namespace wfa_virtual_people {

// This implementation is a copy and formatting of Figure 1 on page 2 in the
// published paper:
//   https://arxiv.org/pdf/1406.2294.pdf
int32_t JumpConsistentHash(uint64_t key, int32_t num_buckets) {
  int32_t b = -1;
  // Use int64_t for j to handle int32_t overflow.
  for (int64_t j = 0; j < num_buckets;) {
    b = j;
    key = key * 2862933555777941757ULL + 1;
    j = (j + 1) *
        (static_cast<double>(1LL << 31) / static_cast<double>((key >> 33) + 1));
  }
  return b;
}

}  // namespace wfa_virtual_people
