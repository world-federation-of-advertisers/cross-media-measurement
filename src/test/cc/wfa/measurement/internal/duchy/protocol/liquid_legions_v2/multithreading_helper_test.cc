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

#include <wfa/measurement/common/crypto/constants.h>

#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "common_cpp/testing/status_macros.h"
#include "common_cpp/testing/status_matchers.h"
#include "gtest/gtest.h"
#include "private_join_and_compute/crypto/commutative_elgamal.h"

namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2 {
namespace {

using ::private_join_and_compute::CommutativeElGamal;
using ::wfa::measurement::common::crypto::kGenerateNewParitialCompositeCipher;
using ::wfa::measurement::common::crypto::kGenerateWithNewElGamalPrivateKey;
using ::wfa::measurement::common::crypto::kGenerateWithNewElGamalPublicKey;
using ::wfa::measurement::common::crypto::kGenerateWithNewPohligHellmanKey;
using ::wfa::measurement::common::crypto::ProtocolCryptorOptions;
using ::wfa::measurement::internal::duchy::protocol::liquid_legions_v2::
    MultithreadingHelper;

constexpr int kThreadCount = 3;
constexpr int kTestCurveId = NID_X9_62_prime256v1;
const std::vector<int> kIntegerData = {1, 2, 3, 4, 5};  // 7 characters;

ProtocolCryptorOptions GetProtocolCryptorOptions() {
  std::unique_ptr<CommutativeElGamal> cipher =
      CommutativeElGamal::CreateWithNewKeyPair(kTestCurveId).value();
  ElGamalCiphertext el_gamal_ciphertext = cipher->GetPublicKeyBytes().value();

  ProtocolCryptorOptions options{
      .curve_id = kTestCurveId,
      .local_el_gamal_public_key = kGenerateWithNewElGamalPublicKey,
      .local_el_gamal_private_key =
          std::string(kGenerateWithNewElGamalPrivateKey),
      .local_pohlig_hellman_private_key =
          std::string(kGenerateWithNewPohligHellmanKey),
      .composite_el_gamal_public_key = el_gamal_ciphertext,
      .partial_composite_el_gamal_public_key =
          kGenerateWithNewElGamalPublicKey};

  return options;
}

TEST(MultithreadingHelper, TasksAreExecutedInMultipleThreads) {
  ASSERT_OK_AND_ASSIGN(auto helper,
                       MultithreadingHelper::CreateMultithreadingHelper(
                           kThreadCount, GetProtocolCryptorOptions()));

  int iteration_count = kIntegerData.size();
  std::vector<int> results_1(iteration_count, -1);

  absl::AnyInvocable<absl::Status(ProtocolCryptor &, size_t)> func_1 =
      [&](ProtocolCryptor &cryptor, size_t index) -> absl::Status {
    results_1[index] = index + kIntegerData[index];
    return absl::OkStatus();
  };
  auto status_1 = helper->Execute(iteration_count, func_1);

  std::vector<int> results_2(iteration_count, -1);
  absl::AnyInvocable<absl::Status(ProtocolCryptor &, size_t)> func_2 =
      [&](ProtocolCryptor &cryptor, size_t index) -> absl::Status {
    results_2[index] = index + kIntegerData[index] + 1;
    return absl::OkStatus();
  };
  auto status_2 = helper->Execute(iteration_count, func_2);

  ASSERT_TRUE(status_1.ok());
  ASSERT_TRUE(status_2.ok());
  EXPECT_EQ(results_1, std::vector<int>({1, 3, 5, 7, 9}));
  EXPECT_EQ(results_2, std::vector<int>({2, 4, 6, 8, 10}));
}

TEST(MultithreadingHelper, TasksAreExecutedInSingleThread) {
  ASSERT_OK_AND_ASSIGN(auto helper,
                       MultithreadingHelper::CreateMultithreadingHelper(
                           1, GetProtocolCryptorOptions()));

  int iteration_count = kIntegerData.size();
  std::vector<int> results(iteration_count, -1);
  absl::AnyInvocable<absl::Status(ProtocolCryptor &, size_t)> func =
      [&](ProtocolCryptor &cryptor, size_t index) -> absl::Status {
    results[index] = index + kIntegerData[index];
    return absl::OkStatus();
  };

  auto status = helper->Execute(iteration_count, func);

  ASSERT_TRUE(status.ok());
  EXPECT_EQ(results, std::vector<int>({1, 3, 5, 7, 9}));
}

TEST(MultithreadingHelper, ErrorReturnedWhenExecutionFails) {
  ASSERT_OK_AND_ASSIGN(auto helper,
                       MultithreadingHelper::CreateMultithreadingHelper(
                           kThreadCount, GetProtocolCryptorOptions()));

  std::string_view error_message = "Internal error.";

  int iteration_count = kIntegerData.size();
  std::vector<int> results(kIntegerData.size(), -1);
  absl::AnyInvocable<absl::Status(ProtocolCryptor &, size_t)> func =
      [&](ProtocolCryptor &cryptor, size_t index) -> absl::Status {
    if (index == 3) {
      return absl::InternalError(error_message);
    } else {
      results[index] = index;
      return absl::OkStatus();
    }
  };

  auto status = helper->Execute(iteration_count, func);
  ASSERT_FALSE(status.ok());
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kInternal, error_message));
  EXPECT_EQ(results[3], -1);
}

}  // namespace
}  // namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2
