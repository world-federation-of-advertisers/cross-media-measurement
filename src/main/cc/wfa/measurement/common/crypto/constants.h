// Copyright 2020 The Cross-Media Measurement Authors
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

#ifndef SRC_MAIN_CC_WFA_MEASUREMENT_COMMON_CRYPTO_CONSTANTS_H_
#define SRC_MAIN_CC_WFA_MEASUREMENT_COMMON_CRYPTO_CONSTANTS_H_

#include <string>
#include <utility>

#include "absl/strings/string_view.h"

namespace wfa::measurement::common::crypto {

inline constexpr int kBytesPerEcPoint = 33;
// A ciphertext contains 2 EcPoints.
inline constexpr int kBytesPerCipherText = kBytesPerEcPoint * 2;
// A register contains 3 ciphertexts, i.e., (index, key, count)
inline constexpr int kBytesPerCipherRegister = kBytesPerCipherText * 3;
// A flags count tuple contains 4 ciphertexts, i.e., (flag_a, flag_b, flag_c,
// count).
inline constexpr int kBytesPerFlagsCountTuple = kBytesPerCipherText * 4;
// The seed for flag value 0. We need such a value since ECPoint 0 is illegal.
inline constexpr absl::string_view kFlagZeroBase = "flag_zero_base";
// The seed for the unit ECPoint in addition.
inline constexpr absl::string_view kUnitECPointSeed = "unit_ec_point";
// The seed for the EcPoint denoting the DestroyedRegisterKey constant.
inline constexpr absl::string_view kDestroyedRegisterKey =
    "destroyed_register_key";
// The seed for the EcPoint denoting the BlindedHistogramNoiseRegisterKey
// constant.
inline constexpr absl::string_view kBlindedHistogramNoiseRegisterKey =
    "blinded_histogram_noise_register_key";
// The seed for the EcPoint denoting the publisher noise register id.
inline constexpr absl::string_view kPublisherNoiseRegisterId =
    "publisher_noise_register_id";
// The seed for the EcPoint denoting the padding noise register id.
inline constexpr absl::string_view kPaddingNoiseRegisterId =
    "padding_noise_register_id";

inline constexpr absl::string_view kGenerateWithNewPohligHellmanKey = "";
inline constexpr absl::string_view kGenerateWithNewElGamalPrivateKey = "";
inline const std::pair<std::string, std::string>
    kGenerateWithNewElGamalPublicKey = {"", ""};
inline const std::pair<std::string, std::string> kGenerateNewCompositeCipher = {
    "", ""};
inline const std::pair<std::string, std::string>
    kGenerateNewParitialCompositeCipher = {"", ""};

}  // namespace wfa::measurement::common::crypto

#endif  // SRC_MAIN_CC_WFA_MEASUREMENT_COMMON_CRYPTO_CONSTANTS_H_
