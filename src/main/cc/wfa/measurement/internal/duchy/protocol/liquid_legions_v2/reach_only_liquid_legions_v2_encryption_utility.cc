// Copyright 2023 The Cross-Media Measurement Authors
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

#include "wfa/measurement/internal/duchy/protocol/liquid_legions_v2/reach_only_liquid_legions_v2_encryption_utility.h"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/algorithm/container.h"
#include "absl/memory/memory.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "common_cpp/macros/macros.h"
#include "common_cpp/time/started_thread_cpu_timer.h"
#include "estimation/estimators.h"
#include "math/distributed_noiser.h"
#include "private_join_and_compute/crypto/commutative_elgamal.h"
#include "wfa/measurement/common/crypto/constants.h"
#include "wfa/measurement/common/crypto/encryption_utility_helper.h"
#include "wfa/measurement/common/crypto/protocol_cryptor.h"
#include "wfa/measurement/common/string_block_sorter.h"
#include "wfa/measurement/internal/duchy/protocol/common/noise_parameters_computation.h"
#include "wfa/measurement/internal/duchy/protocol/liquid_legions_v2/multithreading_helper.h"

namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2 {

namespace {

using ::private_join_and_compute::BigNum;
using ::private_join_and_compute::CommutativeElGamal;
using ::private_join_and_compute::Context;
using ::private_join_and_compute::ECGroup;
using ::wfa::measurement::common::SortStringByBlock;
using ::wfa::measurement::common::crypto::Action;
using ::wfa::measurement::common::crypto::CompositeType;
using ::wfa::measurement::common::crypto::CreateProtocolCryptor;
using ::wfa::measurement::common::crypto::ElGamalCiphertext;
using ::wfa::measurement::common::crypto::ElGamalEcPointPair;
using ::wfa::measurement::common::crypto::ExtractElGamalCiphertextFromString;
using ::wfa::measurement::common::crypto::ExtractKeyCountPairFromRegisters;
using ::wfa::measurement::common::crypto::GetCountValuesPlaintext;
using ::wfa::measurement::common::crypto::GetEcPointPairFromString;
using ::wfa::measurement::common::crypto::GetElGamalEcPoints;
using ::wfa::measurement::common::crypto::GetNumberOfBlocks;
using ::wfa::measurement::common::crypto::kBlindedHistogramNoiseRegisterKey;
using ::wfa::measurement::common::crypto::kBytesPerCipherText;
using ::wfa::measurement::common::crypto::kBytesPerEcPoint;
using ::wfa::measurement::common::crypto::kBytesPerFlagsCountTuple;
using ::wfa::measurement::common::crypto::kDefaultEllipticCurveId;
using ::wfa::measurement::common::crypto::kDestroyedRegisterKey;
using ::wfa::measurement::common::crypto::KeyCountPairCipherText;
using ::wfa::measurement::common::crypto::kFlagZeroBase;
using ::wfa::measurement::common::crypto::kGenerateNewCompositeCipher;
using ::wfa::measurement::common::crypto::kGenerateNewParitialCompositeCipher;
using ::wfa::measurement::common::crypto::kGenerateWithNewElGamalPrivateKey;
using ::wfa::measurement::common::crypto::kGenerateWithNewElGamalPublicKey;
using ::wfa::measurement::common::crypto::kGenerateWithNewPohligHellmanKey;
using ::wfa::measurement::common::crypto::kPaddingNoiseRegisterId;
using ::wfa::measurement::common::crypto::kPublisherNoiseRegisterId;
using ::wfa::measurement::common::crypto::kUnitECPointSeed;
using ::wfa::measurement::common::crypto::MultiplyEcPointPairByScalar;
using ::wfa::measurement::common::crypto::ProtocolCryptor;
using ::wfa::measurement::common::crypto::ProtocolCryptorOptions;
using ::wfa::measurement::internal::duchy::ElGamalPublicKey;
using ::wfa::measurement::internal::duchy::protocol::LiquidLegionsV2NoiseConfig;
using ::wfa::measurement::internal::duchy::protocol::common::
    GetBlindHistogramNoiser;
using ::wfa::measurement::internal::duchy::protocol::common::
    GetGlobalReachDpNoiser;
using ::wfa::measurement::internal::duchy::protocol::common::GetPublisherNoiser;

// Blinds the last layer of ElGamal Encryption of register indexes, and return
// the deterministically encrypted results.
absl::StatusOr<std::vector<std::string>> GetRollv2BlindedRegisterIndexes(
    absl::string_view data, MultithreadingHelper& helper) {
  ASSIGN_OR_RETURN(size_t register_count,
                   GetNumberOfBlocks(data, kBytesPerCipherText));
  std::vector<std::string> blinded_register_indexes;
  blinded_register_indexes.resize(register_count);

  absl::AnyInvocable<absl::Status(ProtocolCryptor&, size_t)> f =
      [&](ProtocolCryptor& cryptor, size_t index) -> absl::Status {
    absl::string_view data_block =
        data.substr(index * kBytesPerCipherText, kBytesPerCipherText);
    ASSIGN_OR_RETURN(ElGamalCiphertext ciphertext,
                     ExtractElGamalCiphertextFromString(data_block));
    ASSIGN_OR_RETURN(std::string decrypted_el_gamal,
                     cryptor.DecryptLocalElGamal(ciphertext));
    blinded_register_indexes[index] = std::move(decrypted_el_gamal);
    return absl::OkStatus();
  };
  RETURN_IF_ERROR(helper.Execute(register_count, f));

  return blinded_register_indexes;
}

absl::StatusOr<int64_t> EstimateReach(double liquid_legions_decay_rate,
                                      int64_t liquid_legions_size,
                                      size_t non_empty_register_count,
                                      float sampling_rate = 1.0) {
  if (liquid_legions_decay_rate <= 1.0) {
    return absl::InvalidArgumentError(absl::StrCat(
        "The decay rate should be > 1, but is ", liquid_legions_decay_rate));
  }
  if (liquid_legions_size <= non_empty_register_count) {
    return absl::InvalidArgumentError(absl::StrCat(
        "liquid legions size (", liquid_legions_size,
        ") should be greater then the number of non empty registers (",
        non_empty_register_count, ")."));
  }
  return wfa::estimation::EstimateCardinalityLiquidLegions(
      liquid_legions_decay_rate, liquid_legions_size, non_empty_register_count,
      sampling_rate);
}

int64_t CountUniqueElements(const std::vector<std::string>& arr) {
  if (arr.empty()) {
    return 0;
  }
  // Create a sorting permutation of the array, such that we don't need to
  // modify the data, whose size could be huge.
  std::vector<size_t> permutation(arr.size());
  absl::c_iota(permutation, 0);
  absl::c_sort(permutation,
               [&](size_t a, size_t b) { return arr[a] < arr[b]; });

  // Counting the number of unique elements by iterating through the indices.
  int64_t count = 1;
  int start = 0;
  for (size_t i = 0; i < arr.size(); ++i) {
    if (arr[permutation[i]] == arr[permutation[start]]) {
      // This register has the same index, it belongs to the same group;
      continue;
    } else {
      // This register belongs to a new group. Increase the unique register
      // count by 1.
      count++;
      // Reset the starting point.
      start = i;
    }
  }
  return count;
}

// Adds encrypted blinded-histogram-noise registers to the end of data.
// returns the number of such noise registers added.
absl::StatusOr<int64_t> AddReachOnlyBlindedHistogramNoise(
    MultithreadingHelper& helper, int total_sketches_count,
    const math::DistributedNoiser& distributed_noiser, size_t pos,
    std::string& data, int64_t& num_unique_noise_id) {
  int64_t noise_register_added = 0;
  num_unique_noise_id = 0;

  std::vector<std::string> register_id_ecs;

  for (int k = 1; k <= total_sketches_count; ++k) {
    // The random number of distinct register_ids that should appear k times.
    ASSIGN_OR_RETURN(int64_t noise_register_count_for_bucket_k,
                     distributed_noiser.GenerateNoiseComponent());
    num_unique_noise_id += noise_register_count_for_bucket_k;

    // Add noise_register_count_for_bucket_k such distinct register ids.
    for (int i = 0; i < noise_register_count_for_bucket_k; ++i) {
      // The prefix is to ensure the value is not in the regular id space.
      std::string register_id =
          absl::StrCat("blinded_histogram_noise",
                       helper.GetProtocolCryptor().NextRandomBigNumAsString());
      ASSIGN_OR_RETURN(std::string register_id_ec,
                       helper.GetProtocolCryptor().MapToCurve(register_id));
      for (int j = 0; j < k; ++j) {
        register_id_ecs.push_back(register_id_ec);
      }
      noise_register_added += k;
    }
  }

  absl::AnyInvocable<absl::Status(ProtocolCryptor&, size_t)> f =
      [&](ProtocolCryptor& cryptor, size_t index) -> absl::Status {
    size_t current_pos = pos + kBytesPerCipherText * index;
    // Add register_id
    RETURN_IF_ERROR(EncryptCompositeElGamalAndWriteToString(
        helper.GetProtocolCryptor(), CompositeType::kFull,
        register_id_ecs[index], current_pos, data));
    current_pos += kBytesPerCipherText;
    return absl::OkStatus();
  };
  RETURN_IF_ERROR(helper.Execute(noise_register_added, f));

  return noise_register_added;
}

// Adds encrypted noise-for-publisher-noise registers to the end of data.
// returns the number of such noise registers added.
absl::StatusOr<int64_t> AddReachOnlyNoiseForPublisherNoise(
    MultithreadingHelper& helper,
    const math::DistributedNoiser& distributed_noiser, size_t pos,
    std::string& data) {
  ASSIGN_OR_RETURN(
      std::string publisher_noise_register_id_ec,
      helper.GetProtocolCryptor().MapToCurve(kPublisherNoiseRegisterId));

  ASSIGN_OR_RETURN(int64_t noise_registers_count,
                   distributed_noiser.GenerateNoiseComponent());
  // Make sure that there is at least one publisher noise added so that we can
  // always subtract 1 for the publisher noise later. This is to avoid the
  // corner case where the noise_registers_count is zero for all workers.
  noise_registers_count++;

  absl::AnyInvocable<absl::Status(ProtocolCryptor&, size_t)> f =
      [&](ProtocolCryptor& cryptor, size_t index) -> absl::Status {
    size_t current_pos = pos + kBytesPerCipherText * index;
    RETURN_IF_ERROR(EncryptCompositeElGamalAndWriteToString(
        cryptor, CompositeType::kFull, publisher_noise_register_id_ec,
        current_pos, data));

    return absl::OkStatus();
  };
  RETURN_IF_ERROR(helper.Execute(noise_registers_count, f));

  return noise_registers_count;
}

// Adds encrypted global-reach-DP-noise registers to the end of data.
// returns the number of such noise registers added.
absl::StatusOr<int64_t> AddReachOnlyGlobalReachDpNoise(
    MultithreadingHelper& helper,
    const math::DistributedNoiser& distributed_noiser, size_t pos,
    std::string& data) {
  ASSIGN_OR_RETURN(int64_t noise_registers_count,
                   distributed_noiser.GenerateNoiseComponent());
  absl::AnyInvocable<absl::Status(ProtocolCryptor&, size_t)> f =
      [&](ProtocolCryptor& cryptor, size_t index) -> absl::Status {
    size_t current_pos = pos + kBytesPerCipherText * index;
    // Add register id, a random number.
    // The prefix is to ensure the value is not in the regular id space.
    std::string register_id =
        absl::StrCat("reach_dp_noise", cryptor.NextRandomBigNumAsString());
    ASSIGN_OR_RETURN(std::string register_id_ec,
                     cryptor.MapToCurve(register_id));
    RETURN_IF_ERROR(EncryptCompositeElGamalAndWriteToString(
        cryptor, CompositeType::kFull, register_id_ec, current_pos, data));

    return absl::OkStatus();
  };
  RETURN_IF_ERROR(helper.Execute(noise_registers_count, f));

  return noise_registers_count;
}

// Adds encrypted padding-noise registers to the end of data.
absl::Status AddReachOnlyPaddingReachNoise(MultithreadingHelper& helper,
                                           int64_t count, size_t pos,
                                           std::string& data) {
  if (count < 0) {
    return absl::InvalidArgumentError("Count should >= 0.");
  }

  ASSIGN_OR_RETURN(
      std::string padding_noise_register_id_ec,
      helper.GetProtocolCryptor().MapToCurve(kPaddingNoiseRegisterId));

  absl::AnyInvocable<absl::Status(ProtocolCryptor&, size_t)> f =
      [&](ProtocolCryptor& cryptor, size_t index) -> absl::Status {
    size_t current_pos = pos + kBytesPerCipherText * index;
    // Add register_id, a predefined constant
    RETURN_IF_ERROR(EncryptCompositeElGamalAndWriteToString(
        cryptor, CompositeType::kFull, padding_noise_register_id_ec,
        current_pos, data));

    return absl::OkStatus();
  };
  RETURN_IF_ERROR(helper.Execute(count, f));
  pos += kBytesPerCipherText * count;

  return absl::OkStatus();
}

absl::Status ValidateReachOnlySetupNoiseParameters(
    const RegisterNoiseGenerationParameters& parameters) {
  if (parameters.contributors_count() < 1) {
    return absl::InvalidArgumentError("contributors_count should be positive.");
  }
  if (parameters.total_sketches_count() < 1) {
    return absl::InvalidArgumentError(
        "total_sketches_count should be positive.");
  }
  if (parameters.dp_params().blind_histogram().epsilon() <= 0 ||
      parameters.dp_params().blind_histogram().delta() <= 0) {
    return absl::InvalidArgumentError(
        "Invalid blind_histogram dp parameter. epsilon/delta should be "
        "positive.");
  }
  if (parameters.dp_params().noise_for_publisher_noise().epsilon() <= 0 ||
      parameters.dp_params().noise_for_publisher_noise().delta() <= 0) {
    return absl::InvalidArgumentError(
        "Invalid noise_for_publisher_noise dp parameter. epsilon/delta should "
        "be positive.");
  }
  if (parameters.dp_params().global_reach_dp_noise().epsilon() <= 0 ||
      parameters.dp_params().global_reach_dp_noise().delta() <= 0) {
    return absl::InvalidArgumentError(
        "Invalid global_reach_dp_noise dp parameter. epsilon/delta should be "
        "positive.");
  }
  return absl::OkStatus();
}

}  // namespace

absl::StatusOr<CompleteReachOnlyInitializationPhaseResponse>
CompleteReachOnlyInitializationPhase(
    const CompleteReachOnlyInitializationPhaseRequest& request) {
  StartedThreadCpuTimer timer;

  ASSIGN_OR_RETURN(
      std::unique_ptr<CommutativeElGamal> cipher,
      CommutativeElGamal::CreateWithNewKeyPair(request.curve_id()));
  ASSIGN_OR_RETURN(ElGamalCiphertext public_key, cipher->GetPublicKeyBytes());
  ASSIGN_OR_RETURN(std::string private_key, cipher->GetPrivateKeyBytes());

  CompleteReachOnlyInitializationPhaseResponse response;
  response.mutable_el_gamal_key_pair()->mutable_public_key()->set_generator(
      public_key.first);
  response.mutable_el_gamal_key_pair()->mutable_public_key()->set_element(
      public_key.second);
  response.mutable_el_gamal_key_pair()->set_secret_key(private_key);

  response.set_elapsed_cpu_time_millis(timer.ElapsedMillis());
  return response;
}

absl::StatusOr<CompleteReachOnlySetupPhaseResponse> CompleteReachOnlySetupPhase(
    const CompleteReachOnlySetupPhaseRequest& request) {
  StartedThreadCpuTimer timer;

  CompleteReachOnlySetupPhaseResponse response;
  std::string* response_crv = response.mutable_combined_register_vector();

  *response_crv = request.combined_register_vector();

  ProtocolCryptorOptions protocol_cryptor_options{
      .curve_id = static_cast<int>(request.curve_id()),
      .local_el_gamal_public_key = kGenerateWithNewElGamalPublicKey,
      .local_el_gamal_private_key =
          std::string(kGenerateWithNewElGamalPrivateKey),
      .local_pohlig_hellman_private_key =
          std::string(kGenerateWithNewPohligHellmanKey),
      .composite_el_gamal_public_key =
          std::make_pair(request.composite_el_gamal_public_key().generator(),
                         request.composite_el_gamal_public_key().element()),
      .partial_composite_el_gamal_public_key =
          kGenerateNewParitialCompositeCipher};

  int64_t excessive_noise_count = 0;

  if (request.has_noise_parameters()) {
    const RegisterNoiseGenerationParameters& noise_parameters =
        request.noise_parameters();

    auto blind_histogram_noiser = GetBlindHistogramNoiser(
        noise_parameters.dp_params().blind_histogram(),
        noise_parameters.contributors_count(), request.noise_mechanism());

    auto publisher_noiser = GetPublisherNoiser(
        noise_parameters.dp_params().noise_for_publisher_noise(),
        noise_parameters.total_sketches_count(),
        noise_parameters.contributors_count(), request.noise_mechanism());

    auto global_reach_dp_noiser = GetGlobalReachDpNoiser(
        noise_parameters.dp_params().global_reach_dp_noise(),
        noise_parameters.contributors_count(), request.noise_mechanism());

    // The total noise registers added. There are additional 2 noise count here
    // to make sure that at least 1 publisher noise and 1 padding noise will be
    // added.
    int64_t total_noise_registers_count =
        publisher_noiser->options().shift_offset * 2 +
        global_reach_dp_noiser->options().shift_offset * 2 +
        blind_histogram_noiser->options().shift_offset *
            noise_parameters.total_sketches_count() *
            (noise_parameters.total_sketches_count() + 1) +
        2;

    // Resize the space to hold all output data.
    size_t pos = response_crv->size();
    response_crv->resize(request.combined_register_vector().size() +
                         total_noise_registers_count * kBytesPerCipherText);

    RETURN_IF_ERROR(ValidateReachOnlySetupNoiseParameters(noise_parameters));
    ASSIGN_OR_RETURN(auto multithreading_helper,
                     MultithreadingHelper::CreateMultithreadingHelper(
                         request.parallelism(), protocol_cryptor_options));

    // 1. Add blinded histogram noise.
    ASSIGN_OR_RETURN(
        int64_t blinded_histogram_noise_count,
        AddReachOnlyBlindedHistogramNoise(
            *multithreading_helper, noise_parameters.total_sketches_count(),
            *blind_histogram_noiser, pos, *response_crv,
            excessive_noise_count));
    pos += kBytesPerCipherText * blinded_histogram_noise_count;
    // 2. Add noise for publisher noise. Publisher noise count is at least 1.
    ASSIGN_OR_RETURN(
        int64_t publisher_noise_count,
        AddReachOnlyNoiseForPublisherNoise(
            *multithreading_helper, *publisher_noiser, pos, *response_crv));
    pos += kBytesPerCipherText * publisher_noise_count;
    // 3. Add reach DP noise.
    ASSIGN_OR_RETURN(int64_t reach_dp_noise_count,
                     AddReachOnlyGlobalReachDpNoise(*multithreading_helper,
                                                    *global_reach_dp_noiser,
                                                    pos, *response_crv));
    pos += kBytesPerCipherText * reach_dp_noise_count;
    // 4. Add padding noise. Padding noise count will be at least 1.
    int64_t padding_noise_count = total_noise_registers_count -
                                  blinded_histogram_noise_count -
                                  publisher_noise_count - reach_dp_noise_count;
    RETURN_IF_ERROR(AddReachOnlyPaddingReachNoise(
        *multithreading_helper, padding_noise_count, pos, *response_crv));
  }

  // Encrypt the excessive noise.
  ASSIGN_OR_RETURN(std::unique_ptr<ProtocolCryptor> protocol_cryptor,
                   CreateProtocolCryptor(protocol_cryptor_options));
  ASSIGN_OR_RETURN(std::string serialized_excessive_noise_ciphertext,
                   protocol_cryptor->EncryptIntegerToStringCompositeElGamal(
                       excessive_noise_count));

  response.set_serialized_excessive_noise_ciphertext(
      serialized_excessive_noise_ciphertext);

  RETURN_IF_ERROR(SortStringByBlock<kBytesPerCipherText>(
      *response.mutable_combined_register_vector()));

  response.set_elapsed_cpu_time_millis(timer.ElapsedMillis());
  return response;
}

absl::StatusOr<CompleteReachOnlySetupPhaseResponse>
CompleteReachOnlySetupPhaseAtAggregator(
    const CompleteReachOnlySetupPhaseRequest& request) {
  StartedThreadCpuTimer timer;
  ASSIGN_OR_RETURN(CompleteReachOnlySetupPhaseResponse response,
                   CompleteReachOnlySetupPhase(request));

  // Get the ElGamal encryption of the excessive noise on the aggregator.
  ASSIGN_OR_RETURN(
      ElGamalEcPointPair ec_point,
      GetEcPointPairFromString(response.serialized_excessive_noise_ciphertext(),
                               request.curve_id()));

  // Combined the excessive_noise ciphertexts.
  int num_ciphertexts = request.serialized_excessive_noise_ciphertext().size() /
                        kBytesPerCipherText;
  for (int i = 0; i < num_ciphertexts; i++) {
    ASSIGN_OR_RETURN(ElGamalEcPointPair temp,
                     GetEcPointPairFromString(
                         request.serialized_excessive_noise_ciphertext().substr(
                             i * kBytesPerCipherText, kBytesPerCipherText),
                         request.curve_id()));
    ASSIGN_OR_RETURN(ec_point, AddEcPointPairs(ec_point, temp));
  }

  std::string excessive_noise_string;
  excessive_noise_string.resize(kBytesPerCipherText);
  RETURN_IF_ERROR(
      WriteEcPointPairToString(ec_point, 0, excessive_noise_string));

  response.set_serialized_excessive_noise_ciphertext(excessive_noise_string);

  response.set_elapsed_cpu_time_millis(timer.ElapsedMillis());
  return response;
}

absl::StatusOr<CompleteReachOnlyExecutionPhaseResponse>
CompleteReachOnlyExecutionPhase(
    const CompleteReachOnlyExecutionPhaseRequest& request) {
  StartedThreadCpuTimer timer;

  ASSIGN_OR_RETURN(size_t register_count,
                   GetNumberOfBlocks(request.combined_register_vector(),
                                     kBytesPerCipherText));

  ProtocolCryptorOptions protocol_cryptor_options{
      .curve_id = static_cast<int>(request.curve_id()),
      .local_el_gamal_public_key = std::make_pair(
          request.local_el_gamal_key_pair().public_key().generator(),
          request.local_el_gamal_key_pair().public_key().element()),
      .local_el_gamal_private_key =
          request.local_el_gamal_key_pair().secret_key(),
      .local_pohlig_hellman_private_key =
          std::string(kGenerateWithNewPohligHellmanKey),
      .composite_el_gamal_public_key = kGenerateNewCompositeCipher,
      .partial_composite_el_gamal_public_key =
          kGenerateNewParitialCompositeCipher};
  ASSIGN_OR_RETURN(auto multithreading_helper,
                   MultithreadingHelper::CreateMultithreadingHelper(
                       request.parallelism(), protocol_cryptor_options));

  CompleteReachOnlyExecutionPhaseResponse response;
  // Partially decrypt the aggregated excessive noise ciphertext.
  ASSIGN_OR_RETURN(std::unique_ptr<ProtocolCryptor> protocol_cryptor,
                   CreateProtocolCryptor(protocol_cryptor_options));

  std::string updated_noise_ciphertext;
  updated_noise_ciphertext.resize(kBytesPerCipherText);
  RETURN_IF_ERROR(protocol_cryptor->BatchProcess(
      request.serialized_excessive_noise_ciphertext(),
      {Action::kPartialDecrypt}, 0, updated_noise_ciphertext));
  response.set_serialized_excessive_noise_ciphertext(updated_noise_ciphertext);

  std::string* response_crv = response.mutable_combined_register_vector();
  // The output crv is the same size with the input crv.
  size_t start_pos = 0;
  response_crv->resize(request.combined_register_vector().size());

  absl::AnyInvocable<absl::Status(ProtocolCryptor&, size_t)> f =
      [&](ProtocolCryptor& cryptor, size_t index) -> absl::Status {
    absl::string_view current_block =
        absl::string_view(request.combined_register_vector())
            .substr(index * kBytesPerCipherText, kBytesPerCipherText);
    size_t pos = start_pos + kBytesPerCipherText * index;

    RETURN_IF_ERROR(cryptor.BatchProcess(current_block, {Action::kBlind}, pos,
                                         *response_crv));

    return absl::OkStatus();
  };

  RETURN_IF_ERROR(multithreading_helper->Execute(register_count, f));
  RETURN_IF_ERROR(SortStringByBlock<kBytesPerCipherText>(*response_crv));

  response.set_elapsed_cpu_time_millis(timer.ElapsedMillis());
  return response;
}

absl::StatusOr<CompleteReachOnlyExecutionPhaseAtAggregatorResponse>
CompleteReachOnlyExecutionPhaseAtAggregator(
    const CompleteReachOnlyExecutionPhaseAtAggregatorRequest& request) {
  StartedThreadCpuTimer timer;

  if (request.combined_register_vector().size() % kBytesPerCipherText != 0) {
    return absl::InvalidArgumentError(absl::StrCat(
        "The size of byte array is not divisible by the block_size: ",
        request.combined_register_vector().size()));
  }

  ProtocolCryptorOptions protocol_cryptor_options{
      .curve_id = static_cast<int>(request.curve_id()),
      .local_el_gamal_public_key = std::make_pair(
          request.local_el_gamal_key_pair().public_key().generator(),
          request.local_el_gamal_key_pair().public_key().element()),
      .local_el_gamal_private_key =
          request.local_el_gamal_key_pair().secret_key(),
      .local_pohlig_hellman_private_key =
          std::string(kGenerateWithNewPohligHellmanKey),
      .composite_el_gamal_public_key = kGenerateNewCompositeCipher,
      .partial_composite_el_gamal_public_key =
          kGenerateNewParitialCompositeCipher};

  // Decrypt the aggregated excessive noise ciphertext to get the excessive
  // noise count.
  int64_t excessive_noise_count = 0;
  ASSIGN_OR_RETURN(std::unique_ptr<ProtocolCryptor> protocol_cryptor,
                   CreateProtocolCryptor(protocol_cryptor_options));
  ASSIGN_OR_RETURN(ElGamalCiphertext ciphertext,
                   ExtractElGamalCiphertextFromString(
                       request.serialized_excessive_noise_ciphertext()));
  ASSIGN_OR_RETURN(
      bool isZero,
      protocol_cryptor->IsDecryptLocalElGamalResultZero(ciphertext));
  if (!isZero) {
    ASSIGN_OR_RETURN(std::string plaintext,
                     protocol_cryptor->DecryptLocalElGamal(ciphertext));

    auto blind_histogram_noiser = GetBlindHistogramNoiser(
        request.noise_parameters().dp_params().blind_histogram(),
        request.noise_parameters().contributors_count(),
        request.noise_mechanism());
    // For each a in [1; number_of_EDPs], each worker samples at most
    // blind_histogram_noiser->options().shift_offset * 2 noise registers. So
    // all workers sample at most #EDPs*#max_per_worker noise registers.
    int max_excessive_noise =
        blind_histogram_noiser->options().shift_offset * 2 *
        request.noise_parameters().total_sketches_count() *
        request.noise_parameters().contributors_count();
    // The lookup table stores the max_excessive_noise EC points where
    // ec_lookup_table[i] = (i+1)*ec_generator.
    ASSIGN_OR_RETURN(
        std::vector<std::string> ec_lookup_table,
        GetCountValuesPlaintext(max_excessive_noise, request.curve_id()));
    // Decrypt the excessive noise using the lookup table.
    int i = 0;
    for (i = 0; i < ec_lookup_table.size(); i++) {
      if (ec_lookup_table[i] == plaintext) {
        excessive_noise_count = i + 1;
        break;
      }
    }
    // Returns an error if the decryption fails.
    if (i == ec_lookup_table.size()) {
      return absl::InternalError(
          "Failed to decrypt the excessive noise ciphertext.");
    }
  }

  ASSIGN_OR_RETURN(auto multithreading_helper,
                   MultithreadingHelper::CreateMultithreadingHelper(
                       request.parallelism(), protocol_cryptor_options));

  ASSIGN_OR_RETURN(
      std::vector<std::string> blinded_register_indexes,
      GetRollv2BlindedRegisterIndexes(request.combined_register_vector(),
                                      *multithreading_helper));
  CompleteReachOnlyExecutionPhaseAtAggregatorResponse response;

  // Counting the number of unique registers.
  int64_t non_empty_register_count =
      CountUniqueElements(blinded_register_indexes);

  // Excluding the blind histogram, padding noise, and the excessive noise from
  // the unique register count. It is guaranteed that if noise is added, then
  // there exist publisher noise and padding noise.
  if (request.has_noise_parameters()) {
    non_empty_register_count -= 2;
  }
  // Remove the total excessive blind histogram noise.
  non_empty_register_count = non_empty_register_count - excessive_noise_count;
  // Remove the reach dp noise baseline.
  if (request.has_reach_dp_noise_baseline()) {
    auto noiser = GetGlobalReachDpNoiser(
        request.reach_dp_noise_baseline().global_reach_dp_noise(),
        request.reach_dp_noise_baseline().contributors_count(),
        request.noise_mechanism());
    const auto& noise_options = noiser->options();
    int64_t global_reach_dp_noise_baseline =
        noise_options.shift_offset * noise_options.contributor_count;
    non_empty_register_count -= global_reach_dp_noise_baseline;
  }

  // If the noise added is less than the baseline, the non empty register count
  // could be negative. Make sure that it is non-negative.
  if (non_empty_register_count < 0) {
    non_empty_register_count = 0;
  }

  // Estimate the reach
  ASSIGN_OR_RETURN(int64_t reach,
                   EstimateReach(request.sketch_parameters().decay_rate(),
                                 request.sketch_parameters().size(),
                                 non_empty_register_count,
                                 request.vid_sampling_interval_width()));

  response.set_reach(reach);
  response.set_elapsed_cpu_time_millis(timer.ElapsedMillis());
  return response;
}

}  // namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2
