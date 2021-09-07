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

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "common_cpp/testing/status_macros.h"
#include "gtest/gtest.h"
#include "tink/util/secret_data.h"
#include "wfa/panelmatch/common/crypto/key_loader.h"

namespace wfa::panelmatch::common::crypto {
namespace {
using ::crypto::tink::util::SecretData;
using ::crypto::tink::util::SecretDataAsStringView;

TEST(IdentityKeyLoaderTest, RegistersAnIdentityKeyLoader) {
  ASSERT_OK_AND_ASSIGN(SecretData key_material, LoadKey("abc"));
  EXPECT_EQ(SecretDataAsStringView(key_material), "abc");
}
}  // namespace
}  // namespace wfa::panelmatch::common::crypto
