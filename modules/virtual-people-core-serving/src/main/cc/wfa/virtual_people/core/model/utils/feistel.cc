// Copyright 2026 The Cross-Media Measurement Authors
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

#include "wfa/virtual_people/core/model/utils/feistel.h"

#include <cmath>
#include <cstdint>
#include <string>

#include "absl/strings/str_cat.h"
#include "src/farmhash.h"

namespace wfa_virtual_people {

uint64_t FeistelPermute(uint64_t value, uint64_t domain_size,
                        const std::string& seed) {
  if (domain_size <= 1) return 0;

  uint64_t half = static_cast<uint64_t>(
      std::ceil(std::sqrt(static_cast<double>(domain_size))));
  uint64_t current = value;

  // Cycle-walk iteratively until the result is in range.
  do {
    uint64_t left = current / half;
    uint64_t right = current % half;

    for (int round = 0; round < 4; ++round) {
      std::string round_input =
          absl::StrCat(seed, "-feistel-", round, "-", right);
      uint64_t round_hash =
          util::Fingerprint64(round_input.data(), round_input.size());
      uint64_t new_right = (left + (round_hash % half)) % half;
      left = right;
      right = new_right;
    }

    current = left * half + right;
  } while (current >= domain_size);

  return current;
}

}  // namespace wfa_virtual_people
