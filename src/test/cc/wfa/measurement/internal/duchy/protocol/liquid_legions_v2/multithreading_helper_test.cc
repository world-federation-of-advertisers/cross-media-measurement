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
using ::wfa::measurement::internal::duchy::protocol::liquid_legions_v2::
    MultithreadingHelper;

constexpr int kThreadCount = 3;
constexpr int kTestCurveId = NID_X9_62_prime256v1;
constexpr absl::string_view kTestData = "abcdefg";  // 7 characters;

absl::StatusOr<ElGamalCiphertext> getFakeElGamalPublicKey() {
  std::unique_ptr<CommutativeElGamal> cipher =
      CommutativeElGamal::CreateWithNewKeyPair(kTestCurveId).value();
  ElGamalCiphertext el_gamal_ciphertext = cipher->GetPublicKeyBytes().value();
  return el_gamal_ciphertext;
}

TEST(MultithreadingHelper, TasksAreExecutedInMultipleThreads) {
  auto composite_el_gamal_public_key = getFakeElGamalPublicKey().value();
  std::string data{kTestData};

  ASSERT_OK_AND_ASSIGN(
      auto helper,
      MultithreadingHelper::CreateMultithreadingHelper(
          kThreadCount, kTestCurveId, kGenerateWithNewElGamalPublicKey,
          kGenerateWithNewElGamalPrivateKey, kGenerateWithNewPohligHellmanKey,
          composite_el_gamal_public_key, kGenerateNewParitialCompositeCipher));

  std::vector<int> results(data.size(), -1);
  absl::AnyInvocable<absl::Status(ProtocolCryptor &, std::string &, size_t)>
      func = [&](ProtocolCryptor &cryptor, std::string &data,
                 size_t index) -> absl::Status {
    results[index] = index;
    return absl::OkStatus();
  };
  auto status = helper->Execute<std::string>(data, data.size(), func);
  ASSERT_TRUE(status.ok());
  EXPECT_EQ(results, std::vector<int>({0, 1, 2, 3, 4, 5, 6}));
}

TEST(MultithreadingHelper, TasksAreExecutedInSingleThread) {
  auto composite_el_gamal_public_key = getFakeElGamalPublicKey().value();
  std::string data{kTestData};

  ASSERT_OK_AND_ASSIGN(
      auto helper,
      MultithreadingHelper::CreateMultithreadingHelper(
          1, kTestCurveId, kGenerateWithNewElGamalPublicKey,
          kGenerateWithNewElGamalPrivateKey, kGenerateWithNewPohligHellmanKey,
          composite_el_gamal_public_key, kGenerateNewParitialCompositeCipher));

  std::vector<int> results(data.size(), -1);
  absl::AnyInvocable<absl::Status(ProtocolCryptor &, std::string &, size_t)>
      func = [&](ProtocolCryptor &cryptor, std::string &data,
                 size_t index) -> absl::Status {
    results[index] = index;
    return absl::OkStatus();
  };
  auto status = helper->Execute<std::string>(data, data.size(), func);
  ASSERT_TRUE(status.ok());
  EXPECT_EQ(results, std::vector<int>({0, 1, 2, 3, 4, 5, 6}));
}

TEST(MultithreadingHelper, ErrorReturnedWhenExecutionFails) {
  auto composite_el_gamal_public_key = getFakeElGamalPublicKey().value();
  std::string data{kTestData};

  ASSERT_OK_AND_ASSIGN(
      auto helper,
      MultithreadingHelper::CreateMultithreadingHelper(
          kThreadCount, kTestCurveId, kGenerateWithNewElGamalPublicKey,
          kGenerateWithNewElGamalPrivateKey, kGenerateWithNewPohligHellmanKey,
          composite_el_gamal_public_key, kGenerateNewParitialCompositeCipher));

  std::vector<int> results(data.size(), -1);
  std::string_view error_message = "Internal error.";
  absl::AnyInvocable<absl::Status(ProtocolCryptor &, std::string &, size_t)>
      func = [&](ProtocolCryptor &cryptor, std::string &data,
                 size_t index) -> absl::Status {
    if (index == 3) {
      return absl::InternalError(error_message);
    } else {
      results[index] = index;
      return absl::OkStatus();
    }
  };
  auto status = helper->Execute<std::string>(data, data.size(), func);
  ASSERT_FALSE(status.ok());
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kInternal, error_message));
  EXPECT_EQ(results[3], -1);
}

}  // namespace
}  // namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2
