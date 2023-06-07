/*
 * Copyright 2021 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SRC_MAIN_CC_WFA_PANELMATCH_COMMON_CRYPTO_KEY_LOADER_H_
#define SRC_MAIN_CC_WFA_PANELMATCH_COMMON_CRYPTO_KEY_LOADER_H_

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "tink/util/secret_data.h"

// This file provides a mechanism for customizing how key material is loaded. It
// is used in situations where dependency injection is not convenient because,
// for example, the C++ code is called from the JVM via JNI.
//
// Libraries that wish to support accessing the global KeyLoader should call
// GetGlobalKeyLoader() and verify it's not null before using it.
//
// To set the global key loader from another library, the Bazel cc_library
// should have alwaylink=True enabled and put this in a library somewhere:
//
//   static auto registration = RegisterGlobalKeyLoader(new MyCustomKeyLoader);

namespace wfa::panelmatch::common::crypto {

// Loads key material by name.
class KeyLoader {
 public:
  virtual ~KeyLoader() = default;

  virtual absl::StatusOr<::crypto::tink::util::SecretData> LoadKey(
      absl::string_view key_name) = 0;
};

// Takes ownership over `key_loader` and sets it to be the KeyLoader used by
// `LoadKey` for `name`. It is an error to call this multiple times.
//
// The destructor of `key_loader` will never be called.
//
// The return value will always be true. While using void would be nice, that
// would make it harder to use because the result of this needs to be assigned
// to a static variable.
bool RegisterGlobalKeyLoader(KeyLoader* key_loader);

// Returns nullptr if RegisterGlobalKeyLoader was never called or its parameter
// if it was called once. The caller does NOT own the return value.
KeyLoader* GetGlobalKeyLoader();

// If GetGlobalKeyLoader() is null, returns an error. Otherwise, calls LoadKey
// on it with the given `key_name`.
absl::StatusOr<::crypto::tink::util::SecretData> LoadKey(
    absl::string_view key_name);

// Removes any registered KeyLoader. Exposed for testing.
void ClearGlobalKeyLoader();

}  // namespace wfa::panelmatch::common::crypto

#endif  // SRC_MAIN_CC_WFA_PANELMATCH_COMMON_CRYPTO_KEY_LOADER_H_
