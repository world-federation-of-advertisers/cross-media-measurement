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

#include "wfa/measurement/internal/duchy/protocol/liquid_legions_v2/multithreading_helper.h"

#include <utility>

#include "absl/memory/memory.h"

namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2 {

using ::wfa::measurement::common::crypto::CreateProtocolCryptorWithKeys;
using ::wfa::measurement::common::crypto::ElGamalCiphertext;

absl::StatusOr<std::unique_ptr<MultithreadingHelper>>
MultithreadingHelper::CreateMultithreadingHelper(
    int n_threads, int curve_id,
    const ElGamalCiphertext& local_el_gamal_public_key,
    absl::string_view local_el_gamal_private_key,
    absl::string_view local_pohlig_hellman_private_key,
    const ElGamalCiphertext& composite_el_gamal_public_key,
    const ElGamalCiphertext& partial_composite_el_gamal_public_key) {
  std::unique_ptr<MultithreadingHelper> helper =
      absl::WrapUnique(new MultithreadingHelper(n_threads));
  RETURN_IF_ERROR(helper->SetupCryptors(
      curve_id, local_el_gamal_public_key, local_el_gamal_private_key,
      local_pohlig_hellman_private_key, composite_el_gamal_public_key,
      partial_composite_el_gamal_public_key));
  return {std::move(helper)};
}

absl::Status MultithreadingHelper::SetupCryptors(
    int curve_id, const ElGamalCiphertext& local_el_gamal_public_key,
    absl::string_view local_el_gamal_private_key,
    absl::string_view local_pohlig_hellman_private_key,
    const ElGamalCiphertext& composite_el_gamal_public_key,
    const ElGamalCiphertext& partial_composite_el_gamal_public_key) {
  for (size_t i = 0; i < num_threads_; i++) {
    ASSIGN_OR_RETURN(
        auto cryptor,
        CreateProtocolCryptorWithKeys(
            curve_id, local_el_gamal_public_key, local_el_gamal_private_key,
            local_pohlig_hellman_private_key, composite_el_gamal_public_key,
            partial_composite_el_gamal_public_key));
    cryptors_.emplace_back(std::move(cryptor));
  }
  return absl::OkStatus();
}

}  // namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2
