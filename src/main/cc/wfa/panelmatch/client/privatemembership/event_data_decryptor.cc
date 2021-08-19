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

#include "wfa/panelmatch/client/privatemembership/event_data_decryptor.h"

#include <memory>
#include <string>
#include <utility>

#include "absl/memory/memory.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "common_cpp/macros/macros.h"
#include "tink/util/secret_data.h"
#include "wfa/panelmatch/common/crypto/aes.h"
#include "wfa/panelmatch/common/crypto/aes_with_hkdf.h"
#include "wfa/panelmatch/common/crypto/hkdf.h"

namespace wfa::panelmatch::client::privatemembership {
using ::crypto::tink::util::SecretData;
using ::crypto::tink::util::SecretDataAsStringView;
using ::crypto::tink::util::SecretDataFromStringView;
using ::wfa::panelmatch::common::crypto::Aes;
using ::wfa::panelmatch::common::crypto::AesWithHkdf;
using ::wfa::panelmatch::common::crypto::GetAesSivCmac512;
using ::wfa::panelmatch::common::crypto::GetSha256Hkdf;
using ::wfa::panelmatch::common::crypto::Hkdf;

absl::StatusOr<DecryptEventDataResponse> DecryptEventData(
    const DecryptEventDataRequest& request) {
  if (request.hkdf_pepper().empty()) {
    return absl::InvalidArgumentError("Empty HKDF Pepper");
  }
  if (request.single_blinded_joinkey().empty()) {
    return absl::InvalidArgumentError("Empty Single Blinded Joinkey");
  }
  std::unique_ptr<Aes> aes = GetAesSivCmac512();
  std::unique_ptr<Hkdf> hkdf = GetSha256Hkdf();
  const AesWithHkdf aes_hkdf = AesWithHkdf(std::move(hkdf), std::move(aes));
  DecryptEventDataResponse decrypted_event_data;
  for (const std::string& encrypted_event : request.encrypted_event_data()) {
    ASSIGN_OR_RETURN(
        *decrypted_event_data.add_decrypted_event_data(),
        aes_hkdf.Decrypt(
            encrypted_event,
            SecretDataFromStringView(request.single_blinded_joinkey()),
            SecretDataFromStringView(request.hkdf_pepper())));
  }
  return decrypted_event_data;
}
}  // namespace wfa::panelmatch::client::privatemembership
