// Copyright 2021 The Cross-Media Measurement Authors
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

#include "wfa/panelmatch/common/crypto/key_loader.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "common_cpp/testing/status_macros.h"
#include "gtest/gtest.h"
#include "tink/util/secret_data.h"

namespace wfa::panelmatch::common::crypto {
namespace {
using ::crypto::tink::util::SecretData;
using ::crypto::tink::util::SecretDataFromStringView;

SecretData GetTestSecretData(absl::string_view key_name) {
  return SecretDataFromStringView(absl::StrCat("Key material for: ", key_name));
}

class FakeKeyLoader : public KeyLoader {
 public:
  absl::StatusOr<SecretData> LoadKey(absl::string_view key_name) override {
    return GetTestSecretData(key_name);
  }
};

auto* some_fake_key_loader = new FakeKeyLoader;

auto unused = RegisterGlobalKeyLoader(some_fake_key_loader);

TEST(KeyLoaderTest, FakeKeyLoader) {
  EXPECT_EQ(GetGlobalKeyLoader(), some_fake_key_loader);
  ASSERT_OK_AND_ASSIGN(SecretData result, LoadKey("abc"));
  EXPECT_EQ(result, GetTestSecretData("abc"));
}

TEST(KeyLoaderTest, NoKeyLoader) {
  ClearGlobalKeyLoader();
  EXPECT_EQ(GetGlobalKeyLoader(), nullptr);
  RegisterGlobalKeyLoader(some_fake_key_loader);
}

TEST(KeyLoaderDeathTest, Reregister) {
  EXPECT_DEATH(RegisterGlobalKeyLoader(new FakeKeyLoader), "");
}

TEST(KeyLoaderDeathTest, RegisterNullptr) {
  EXPECT_DEATH(RegisterGlobalKeyLoader(nullptr), "");
}

}  // namespace
}  // namespace wfa::panelmatch::common::crypto
