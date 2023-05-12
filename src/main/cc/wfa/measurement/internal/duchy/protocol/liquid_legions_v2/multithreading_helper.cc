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

#include "absl/memory/memory.h"

namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2 {

using ::wfa::measurement::common::crypto::CreateProtocolCryptorWithKeys;
using ::wfa::measurement::common::crypto::ElGamalCiphertext;
using ::wfa::measurement::common::crypto::ProtocolCryptorKeys;

absl::StatusOr<std::unique_ptr<MultithreadingHelper>>
MultithreadingHelper::CreateMultithreadingHelper(
    int num_threads, int curve_id,
    const ElGamalCiphertext& local_el_gamal_public_key,
    absl::string_view local_el_gamal_private_key,
    absl::string_view local_pohlig_hellman_private_key,
    const ElGamalCiphertext& composite_el_gamal_public_key,
    const ElGamalCiphertext& partial_composite_el_gamal_public_key) {
  ABSL_ASSERT(num_threads > 0);

  ASSIGN_OR_RETURN(
      auto cryptors,
      MultithreadingHelper::CreateCryptors(
          num_threads, curve_id, local_el_gamal_public_key,
          local_el_gamal_private_key, local_pohlig_hellman_private_key,
          composite_el_gamal_public_key,
          partial_composite_el_gamal_public_key));
  std::unique_ptr<MultithreadingHelper> helper = absl::WrapUnique(
      new MultithreadingHelper(num_threads, std::move(cryptors)));
  return {std::move(helper)};
}

absl::StatusOr<std::vector<std::unique_ptr<ProtocolCryptor>>>
MultithreadingHelper::CreateCryptors(
    int num, int curve_id, const ElGamalCiphertext& local_el_gamal_public_key,
    absl::string_view local_el_gamal_private_key,
    absl::string_view local_pohlig_hellman_private_key,
    const ElGamalCiphertext& composite_el_gamal_public_key,
    const ElGamalCiphertext& partial_composite_el_gamal_public_key) {
  std::vector<std::unique_ptr<ProtocolCryptor>> cryptors;
  for (size_t i = 0; i < num; i++) {
    ProtocolCryptorKeys keys(
        curve_id, local_el_gamal_public_key, local_el_gamal_private_key,
        local_pohlig_hellman_private_key, composite_el_gamal_public_key,
        partial_composite_el_gamal_public_key);
    ASSIGN_OR_RETURN(auto cryptor, CreateProtocolCryptorWithKeys(keys));
    cryptors.emplace_back(std::move(cryptor));
  }
  return cryptors;
}

absl::Status MultithreadingHelper::Execute(
    int num_iterations,
    absl::AnyInvocable<absl::Status(ProtocolCryptor&, size_t)>& f) {
  failures_ = std::vector<std::optional<absl::Status>>(num_threads_);

  size_t count = num_iterations / num_threads_ + num_iterations % num_threads_;
  size_t start_index = 0;
  for (int thread_index = 0; thread_index < num_threads_; thread_index++) {
    if (thread_index == 1) {
      count = num_iterations / num_threads_;
    }
    threads_.emplace_back(std::thread(&MultithreadingHelper::ExecuteCryptorTask,
                                      this, thread_index, start_index, count,
                                      std::ref(f)));
    start_index += count;
  }
  for (auto& thread : threads_) {
    thread.join();
  }

  for (auto& failure : failures_) {
    if (failure.has_value()) {
      return failure.value();
    }
  }
  return absl::OkStatus();
}

void MultithreadingHelper::ExecuteCryptorTask(
    size_t thread_index, size_t start_index, size_t count,
    absl::AnyInvocable<absl::Status(ProtocolCryptor&, size_t)>& f) {
  ProtocolCryptor& cryptor = *cryptors_[thread_index];

  for (size_t i = 0; i < count; i++) {
    size_t current_index = start_index + i;

    auto status = f(cryptor, current_index);
    if (!status.ok()) {
      failures_[thread_index] = status;
      return;
    }
  }
}

}  // namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2
