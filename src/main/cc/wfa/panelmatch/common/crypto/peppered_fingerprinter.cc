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

#include "wfa/panelmatch/common/crypto/peppered_fingerprinter.h"

#include <memory>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "common_cpp/fingerprinters/fingerprinters.h"
#include "glog/logging.h"
#include "tink/util/secret_data.h"

namespace wfa::panelmatch::common::crypto {
namespace {

using ::crypto::tink::util::SecretData;
using ::crypto::tink::util::SecretDataAsStringView;

// A hashfunction that concatenates a pepper to an input, then uses
// a specified hashfunction.
class PepperedFingerprinter : public wfa::Fingerprinter {
 public:
  ~PepperedFingerprinter() override = default;

  PepperedFingerprinter(const Fingerprinter* delegate, const SecretData& pepper)
      : pepper_(pepper), delegate_(CHECK_NOTNULL(delegate)) {}

  // Uses 'delegate' to hash the concatenation of 'pepper' and 'item'
  uint64_t Fingerprint(absl::Span<const unsigned char> item) const override {
    absl::string_view item_as_string_view(
        reinterpret_cast<const char*>(item.data()), item.size());
    return delegate_->Fingerprint(
        absl::StrCat(item_as_string_view, SecretDataAsStringView(pepper_)));
  }

 private:
  const SecretData pepper_;
  const Fingerprinter* delegate_;
};
}  // namespace

std::unique_ptr<wfa::Fingerprinter> GetPepperedFingerprinter(
    const wfa::Fingerprinter* delegate, const SecretData& pepper) {
  return absl::make_unique<PepperedFingerprinter>(delegate, pepper);
}
}  // namespace wfa::panelmatch::common::crypto
