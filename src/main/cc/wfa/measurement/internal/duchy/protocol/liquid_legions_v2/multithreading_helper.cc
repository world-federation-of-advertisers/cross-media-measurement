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

#include <thread>  // NOLINT(build/c++11)

#include "absl/memory/memory.h"
#include "common_cpp/macros/macros.h"

namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2 {

using ::wfa::measurement::common::crypto::CreateIdenticalProtocolCrypors;
using ::wfa::measurement::common::crypto::CreateProtocolCryptor;
using ::wfa::measurement::common::crypto::ElGamalCiphertext;
using ::wfa::measurement::common::crypto::ProtocolCryptorOptions;

absl::StatusOr<std::unique_ptr<MultithreadingHelper>>
MultithreadingHelper::CreateMultithreadingHelper(
    int num_threads, const ProtocolCryptorOptions& options) {
  if (num_threads <= 0) {
    return absl::InvalidArgumentError("Parallelism must be greater than zero.");
  }

  ASSIGN_OR_RETURN_ERROR(
      auto cryptors, CreateIdenticalProtocolCrypors(num_threads, options),
      "Failed to create the protocol cipher, invalid curveId or keys.");
  // As the constructor of MultithreadingHelper is private, absl::WrapUnique
  // helps to create a std::unique_ptr with `new` to bypass the restriction.
  return absl::WrapUnique(
      new MultithreadingHelper(num_threads, std::move(cryptors)));
}

ProtocolCryptor& MultithreadingHelper::GetProtocolCryptor() {
  return *(cryptors_.front());
}

absl::Status MultithreadingHelper::Execute(
    int num_iterations,
    absl::AnyInvocable<absl::Status(ProtocolCryptor&, size_t)>& f) {
  std::vector<std::thread> threads;
  std::vector<std::optional<absl::Status>> failures(num_threads_);

  size_t start_index = 0;
  for (int thread_index = 0; thread_index < num_threads_; thread_index++) {
    // If num_iterations % num_threads_ != 0, former threads should take 1 more
    // task than latter threads.
    size_t count = num_iterations / num_threads_ +
                   (thread_index < num_iterations % num_threads_ ? 1 : 0);

    threads.emplace_back(std::thread(
        &MultithreadingHelper::ExecuteCryptorTask, this, thread_index,
        start_index, count, std::ref(failures[thread_index]), std::ref(f)));
    start_index += count;
  }
  for (auto& thread : threads) {
    thread.join();
  }

  for (auto& failure : failures) {
    if (failure.has_value()) {
      return failure.value();
    }
  }
  return absl::OkStatus();
}

void MultithreadingHelper::ExecuteCryptorTask(
    size_t thread_index, size_t start_index, size_t count,
    std::optional<absl::Status>& failure,
    absl::AnyInvocable<absl::Status(ProtocolCryptor&, size_t)>& f) {
  ProtocolCryptor& cryptor = *cryptors_[thread_index];

  for (size_t index = start_index; index < start_index + count; index++) {
    auto status = f(cryptor, index);
    if (!status.ok()) {
      failure = status;
      return;
    }
  }
}

}  // namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2
