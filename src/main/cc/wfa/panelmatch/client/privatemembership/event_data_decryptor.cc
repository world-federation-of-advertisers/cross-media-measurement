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
#include <utility>

#include "absl/status/statusor.h"
#include "tink/util/secret_data.h"
#include "wfa/panelmatch/client/exchangetasks/join_key.pb.h"
#include "wfa/panelmatch/client/privatemembership/query.pb.h"
#include "wfa/panelmatch/common/crypto/aes.h"
#include "wfa/panelmatch/common/crypto/aes_with_hkdf.h"
#include "wfa/panelmatch/common/crypto/hkdf.h"

namespace wfa::panelmatch::client::privatemembership {
using ::crypto::tink::util::SecretData;
using ::crypto::tink::util::SecretDataFromStringView;
using ::wfa::panelmatch::common::crypto::Aes;
using ::wfa::panelmatch::common::crypto::AesWithHkdf;
using ::wfa::panelmatch::common::crypto::GetAesSivCmac512;
using ::wfa::panelmatch::common::crypto::GetSha256Hkdf;
using ::wfa::panelmatch::common::crypto::Hkdf;

absl::StatusOr<DecryptedEventDataSet> DecryptEventData(
    const DecryptEventDataRequest& request) {
  if (request.hkdf_pepper().empty()) {
    return absl::InvalidArgumentError("Empty HKDF Pepper");
  }
  std::unique_ptr<Aes> aes = GetAesSivCmac512();
  std::unique_ptr<Hkdf> hkdf = GetSha256Hkdf();
  const AesWithHkdf aes_hkdf = AesWithHkdf(std::move(hkdf), std::move(aes));
  DecryptedEventDataSet response;
  response.mutable_query_id()->set_id(
      request.encrypted_event_data_set().query_id().id());
  auto key = SecretDataFromStringView(request.lookup_key().key());
  for (const std::string& encrypted_event : request.encrypted_event_data_set()
                                                .encrypted_event_data()
                                                .ciphertexts()) {
    absl::StatusOr<std::string> plaintext = aes_hkdf.Decrypt(
        encrypted_event, key, SecretDataFromStringView(request.hkdf_pepper()));
    if (plaintext.ok()) {
      Plaintext* decrypted_event_data = response.add_decrypted_event_data();
      decrypted_event_data->set_payload(*std::move(plaintext));
    }
  }
  return response;
}
}  // namespace wfa::panelmatch::client::privatemembership
