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

#ifndef WFA_MEASUREMENT_COMMON_CRYPTO_ENCRYPTION_UTILITY_HELPER_H_
#define WFA_MEASUREMENT_COMMON_CRYPTO_ENCRYPTION_UTILITY_HELPER_H_

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "wfa/measurement/common/crypto/ec_point_util.h"

namespace wfa::measurement::common::crypto {

// Gets the number of equal sized blocks the data chunk can be partitioned to.
absl::StatusOr<size_t> GetNumberOfBlocks(absl::string_view data,
                                         size_t block_size);

// Extracts an ElGamalCiphertext from a string_view.
absl::StatusOr<ElGamalCiphertext> ExtractElGamalCiphertextFromString(
    absl::string_view str);

// Gets the cpu duration of current thread.
absl::Duration GetCurrentThreadCpuDuration();

}  // namespace wfa::measurement::common::crypto

#endif  // WFA_MEASUREMENT_COMMON_CRYPTO_ENCRYPTION_UTILITY_HELPER_H_