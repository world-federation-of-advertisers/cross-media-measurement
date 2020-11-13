// Copyright 2020 The Measurement System Authors
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

#include "wfa/measurement/common/crypto/encryption_utility_helper.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/time/time.h"
#include "wfa/measurement/common/crypto/constants.h"

namespace wfa::measurement::common::crypto {

absl::StatusOr<size_t> GetNumberOfBlocks(absl::string_view data,
                                         size_t block_size) {
  if (data.empty()) {
    return absl::InvalidArgumentError("Input data is empty");
  }
  if (block_size == 0) {
    return absl::InvalidArgumentError("block_size is zero");
  }
  if (data.size() % block_size) {
    return absl::InvalidArgumentError(absl::StrCat(
        "The size of byte array is not divisible by the row_size: ",
        block_size));
  }
  return data.size() / block_size;
}

absl::StatusOr<ElGamalCiphertext> ExtractElGamalCiphertextFromString(
    absl::string_view str) {
  if (str.size() != kBytesPerCipherText) {
    return absl::InternalError("string size doesn't match ciphertext size.");
  }
  return std::make_pair(
      std::string(str.substr(0, kBytesPerEcPoint)),
      std::string(str.substr(kBytesPerEcPoint, kBytesPerEcPoint)));
}

absl::Duration GetCurrentThreadCpuDuration() {
#ifdef __linux__
  struct timespec ts;
  CHECK(clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts) == 0)
      << "Failed to get the thread cpu time.";
  return absl::DurationFromTimespec(ts);
#else
  return absl::ZeroDuration();
#endif
}

}  // namespace wfa::measurement::common::crypto
