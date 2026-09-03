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

#include "wfa/virtual_people/core/model/utils/hash.h"

#include <cmath>
#include <cstdint>
#include <limits>

#include "absl/strings/string_view.h"
#include "src/farmhash.h"

namespace wfa_virtual_people {

double FloatHash(absl::string_view seed) {
  return static_cast<double>(util::Fingerprint64(seed)) /
         static_cast<double>(std::numeric_limits<uint64_t>::max());
}

double ExpHash(absl::string_view seed) { return -std::log(FloatHash(seed)); }

}  // namespace wfa_virtual_people
