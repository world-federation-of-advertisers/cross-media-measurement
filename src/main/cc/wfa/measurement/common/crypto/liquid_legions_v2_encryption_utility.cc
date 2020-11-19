// Copyright 2020 The Measurement System Authors
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

#include "wfa/measurement/common/crypto/liquid_legions_v2_encryption_utility.h"

#include "absl/status/statusor.h"
#include "wfa/measurement/common/crypto/constants.h"
#include "wfa/measurement/common/crypto/encryption_utility_helper.h"
#include "wfa/measurement/common/crypto/protocol_cryptor.h"
#include "wfa/measurement/common/macros.h"
#include "wfa/measurement/common/string_block_sorter.h"

namespace wfa::measurement::common::crypto {

absl::StatusOr<CompleteReachEstimationPhaseResponse>
CompleteReachEstimationPhase(
    const CompleteReachEstimationPhaseRequest& request) {
  absl::Duration startCpuDuration = GetCurrentThreadCpuDuration();

  ASSIGN_OR_RETURN(size_t register_count,
                   GetNumberOfBlocks(request.combined_register_vector(),
                                     kBytesPerCipherRegister));
  ASSIGN_OR_RETURN_ERROR(
      auto protocol_cryptor,
      CreateProtocolCryptorWithKeys(
          request.curve_id(),
          std::make_pair(
              request.local_el_gamal_key_pair().public_key().generator(),
              request.local_el_gamal_key_pair().public_key().element()),
          request.local_el_gamal_key_pair().secret_key(),
          /*local_pohlig_hellman_private_key=*/"",
          std::make_pair(request.composite_el_gamal_public_key().generator(),
                         request.composite_el_gamal_public_key().element())),
      "Failed to create the protocol cipher, invalid curveId or keys.");

  CompleteReachEstimationPhaseResponse response;
  std::string* response_crv = response.mutable_combined_register_vector();
  // The output crv is the same size with the input crv.
  response_crv->reserve(request.combined_register_vector().size());

  for (size_t index_i = 0; index_i < register_count; ++index_i) {
    absl::string_view current_block =
        absl::string_view(request.combined_register_vector())
            .substr(index_i * kBytesPerCipherRegister, kBytesPerCipherRegister);
    RETURN_IF_ERROR(protocol_cryptor->BatchProcess(
        current_block,
        {Action::kBlind, Action::kReRandomize, Action::kReRandomize},
        *response_crv));
  }

  RETURN_IF_ERROR(SortStringByBlock<kBytesPerCipherRegister>(*response_crv));

  absl::Duration elaspedDuration =
      GetCurrentThreadCpuDuration() - startCpuDuration;
  response.set_elapsed_cpu_time_millis(
      absl::ToInt64Milliseconds(elaspedDuration));
  return response;
}

}  // namespace wfa::measurement::common::crypto
