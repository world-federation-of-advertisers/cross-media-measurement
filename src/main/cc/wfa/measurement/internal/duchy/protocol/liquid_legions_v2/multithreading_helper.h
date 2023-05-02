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

#ifndef SRC_MAIN_CC_WFA_MEASUREMENT_INTERNAL_DUCHY_PROTOCOL_LIQUID_LEGIONS_V2_MULTITHREADING_HELPER_H_
#define SRC_MAIN_CC_WFA_MEASUREMENT_INTERNAL_DUCHY_PROTOCOL_LIQUID_LEGIONS_V2_MULTITHREADING_HELPER_H_

#include <memory>
#include <thread>
#include <vector>

#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "wfa/measurement/common/crypto/ec_point_util.h"
#include "wfa/measurement/common/crypto/protocol_cryptor.h"

namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2 {

using ::wfa::measurement::common::crypto::ElGamalCiphertext;
using ::wfa::measurement::common::crypto::ProtocolCryptor;

class MultithreadingHelper {
 private:
  absl::Mutex mutex_;

  int n_threads_;
  std::vector<std::thread> threads_;

  std::vector<std::unique_ptr<ProtocolCryptor>> cryptors_;
  int n_iterations_ = 0;
  int iteration_index_ = 0;

  std::optional<absl::Status> failure_status_;

 public:
  explicit MultithreadingHelper(int n_threads) : n_threads_(n_threads) {}

  // Initialize [ProtocolCryptor] for threads.
  //
  // As [ProtocolCryptor] is not thread-safe, each thread has its own cryptor to
  // execute tasks.
  absl::Status setupCryptors(
      int curve_id, const ElGamalCiphertext& local_el_gamal_public_key,
      absl::string_view local_el_gamal_private_key,
      absl::string_view local_pohlig_hellman_private_key,
      const ElGamalCiphertext& composite_el_gamal_public_key,
      const ElGamalCiphertext& partial_composite_el_gamal_public_key);

  static absl::StatusOr<std::unique_ptr<MultithreadingHelper>>
  createMultithreadingHelper(
      int n_threads, int curve_id,
      const ElGamalCiphertext& local_el_gamal_public_key,
      absl::string_view local_el_gamal_private_key,
      absl::string_view local_pohlig_hellman_private_key,
      const ElGamalCiphertext& composite_el_gamal_public_key,
      const ElGamalCiphertext& partial_composite_el_gamal_public_key);

  template <typename T>
  absl::Status executeAndBlocking(
      T& data, int n_iterations,
      absl::AnyInvocable<absl::Status(ProtocolCryptor&, T&, size_t)>& f) {
    if (cryptors_.size() != n_threads_) {
      return absl::FailedPreconditionError(
          "ProtocolCryptors have not been setup.");
    }

    n_iterations_ = n_iterations;
    iteration_index_ = 0;
    failure_status_.reset();

    for (int thread_index = 0; thread_index < n_threads_; thread_index++) {
      threads_.emplace_back(std::thread(&MultithreadingHelper::execute<T>, this,
                                        std::ref(*cryptors_[thread_index]),
                                        std::ref(data), std::ref(f)));
    }
    for (auto& thread : threads_) {
      thread.join();
    }

    return failure_status_.has_value() ? failure_status_.value()
                                       : absl::OkStatus();
  }

  template <typename T>
  void execute(
      ProtocolCryptor& cryptor, T& data,
      absl::AnyInvocable<absl::Status(ProtocolCryptor&, T&, size_t)>& f) {
    int current_index;
    while (true) {
      {
        absl::MutexLock lock(&mutex_);

        if (iteration_index_ < n_iterations_ && !failure_status_.has_value()) {
          current_index = iteration_index_;
          iteration_index_ += 1;
        } else {
          return;
        }
      }

      auto status = f(cryptor, data, current_index);

      if (!status.ok()) {
        absl::MutexLock lock(&mutex_);
        if (!failure_status_.has_value()) {
          failure_status_ = status;
        }
        return;
      }
    }
  }
};

}  // namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2

#endif  // SRC_MAIN_CC_WFA_MEASUREMENT_INTERNAL_DUCHY_PROTOCOL_LIQUID_LEGIONS_V2_MULTITHREADING_HELPER_H_
