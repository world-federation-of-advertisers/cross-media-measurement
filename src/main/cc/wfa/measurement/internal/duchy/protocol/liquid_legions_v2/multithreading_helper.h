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
#include <utility>
#include <vector>

#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "wfa/measurement/common/crypto/ec_point_util.h"
#include "wfa/measurement/common/crypto/protocol_cryptor.h"

namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2 {

using ::wfa::measurement::common::crypto::ElGamalCiphertext;
using ::wfa::measurement::common::crypto::ProtocolCryptor;
using ::wfa::measurement::common::crypto::ProtocolCryptorOptions;

// A helper class to execute cryptor iterations in multiple threads.
class MultithreadingHelper {
 private:
  explicit MultithreadingHelper(
      int num_threads, std::vector<std::unique_ptr<ProtocolCryptor>> cryptors)
      : num_threads_(num_threads), cryptors_(std::move(cryptors)) {}

  static absl::StatusOr<std::vector<std::unique_ptr<ProtocolCryptor>>>
  CreateCryptors(int num, const ProtocolCryptorOptions& options);

  const int num_threads_;
  const std::vector<std::unique_ptr<ProtocolCryptor>> cryptors_;

  void ExecuteCryptorTask(
      size_t thread_index, size_t start_index, size_t count,
      std::optional<absl::Status>& failure,
      absl::AnyInvocable<absl::Status(ProtocolCryptor&, size_t)>& f);

 public:
  static absl::StatusOr<std::unique_ptr<MultithreadingHelper>>
  CreateMultithreadingHelper(int num_threads,
                             const ProtocolCryptorOptions& options);

  // Execute function f with a batch input [data] and number of iterations.
  // Note: The function f must be thread-safe.
  //
  // Example:
  //  std::string data = "123|456|789|";
  //  size_t iteration_count = 3;
  //  std::vector<std::string> results(iteration_count, "");
  //  absl::AnyInvocable<absl::Status(ProtocolCryptor &, size_t)>
  //      f = [&](ProtocolCryptor &cryptor, size_t index) -> absl::Status {
  //    string random = cryptor.NextRandomBigNumAsString()
  //    results[index] = random + data[index];
  //    return absl::OkStatus();
  //  };
  //  multithreading_helper.Execute(iteration_count, f);

  absl::Status Execute(
      int num_iterations,
      absl::AnyInvocable<absl::Status(ProtocolCryptor&, size_t)>& f);

  // Returns a reference to a protocol cryptor.
  ProtocolCryptor& GetProtocolCryptor();
};

}  // namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2

#endif  // SRC_MAIN_CC_WFA_MEASUREMENT_INTERNAL_DUCHY_PROTOCOL_LIQUID_LEGIONS_V2_MULTITHREADING_HELPER_H_
