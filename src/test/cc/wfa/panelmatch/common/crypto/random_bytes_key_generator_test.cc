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

#include "wfa/panelmatch/common/crypto/random_bytes_key_generator.h"

#include "common_cpp/testing/status_macros.h"
#include "common_cpp/testing/status_matchers.h"
#include "gtest/gtest.h"

namespace wfa::panelmatch::common::crypto {
namespace {

using ::crypto::tink::util::SecretData;

// Make sure each call to GenerateKey returns a unique value
TEST(RandomBytesKeyGenerator, uniqueValues) {
  RandomBytesKeyGenerator generator;
  ASSERT_OK_AND_ASSIGN(SecretData key1, generator.GenerateKey(32));
  ASSERT_OK_AND_ASSIGN(SecretData key2, generator.GenerateKey(32));
  ASSERT_NE(key1, key2);
}

// Make sure values <= 0 return an error
TEST(RandomBytesKeyGenerator, invalidArgument) {
  RandomBytesKeyGenerator generator;
  auto key = generator.GenerateKey(-12);
  EXPECT_THAT(key.status(), StatusIs(absl::StatusCode::kInvalidArgument,
                                     "Size must be greater than 0"));
}

}  // namespace
}  // namespace wfa::panelmatch::common::crypto
