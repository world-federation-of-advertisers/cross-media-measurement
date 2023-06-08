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

#include <string>

#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "glog/logging.h"
#include "tink/util/secret_data.h"

namespace wfa::panelmatch::common::crypto {
namespace {
using ::crypto::tink::util::SecretData;

class KeyLoaderRegistry {
 public:
  KeyLoaderRegistry() : key_loader_(nullptr) {}

  void Set(KeyLoader* key_loader) {
    absl::MutexLock lock(&mu_);
    CHECK(key_loader != nullptr);
    CHECK(key_loader_ == nullptr || key_loader_ == key_loader);
    key_loader_ = key_loader;
  }

  KeyLoader* Get() {
    absl::MutexLock lock(&mu_);
    return key_loader_;
  }

  void Clear() {
    absl::MutexLock lock(&mu_);
    delete key_loader_;
    key_loader_ = nullptr;
  }

 private:
  absl::Mutex mu_;
  KeyLoader* key_loader_ ABSL_GUARDED_BY(mu_);
};

KeyLoaderRegistry& GetGlobalKeyLoaderRegistry() {
  static auto* const registry = new KeyLoaderRegistry;
  return *registry;
}
}  // namespace

bool RegisterGlobalKeyLoader(KeyLoader* key_loader) {
  GetGlobalKeyLoaderRegistry().Set(key_loader);
  return true;
}

KeyLoader* GetGlobalKeyLoader() { return GetGlobalKeyLoaderRegistry().Get(); }

void ClearGlobalKeyLoader() { GetGlobalKeyLoaderRegistry().Clear(); }

absl::StatusOr<SecretData> LoadKey(absl::string_view key_name) {
  KeyLoader* key_loader = GetGlobalKeyLoader();
  if (key_loader == nullptr) {
    return absl::InternalError("No KeyLoader is configured");
  }
  return key_loader->LoadKey(key_name);
}
}  // namespace wfa::panelmatch::common::crypto
