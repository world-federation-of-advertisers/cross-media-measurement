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

#include "wfa/panelmatch/common/crypto/identity_key_loader.h"

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "tink/util/secret_data.h"
#include "wfa/panelmatch/common/crypto/key_loader.h"

namespace wfa::panelmatch::common::crypto {
namespace {
class IdentityKeyLoader : public KeyLoader {
  virtual absl::StatusOr<::crypto::tink::util::SecretData> LoadKey(
      absl::string_view key_name) {
    return ::crypto::tink::util::SecretDataFromStringView(key_name);
  }
};
}  // namespace

void RegisterIdentityKeyLoader() {
  static auto* key_loader = new IdentityKeyLoader;
  RegisterGlobalKeyLoader(key_loader);
}
}  // namespace wfa::panelmatch::common::crypto
