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

#ifndef WFA_MEASUREMENT_COMMON_CRYPTO_EC_POINT_UTIL_H_
#define WFA_MEASUREMENT_COMMON_CRYPTO_EC_POINT_UTIL_H_

#include <memory>

#include "crypto/ec_group.h"
#include "crypto/ec_point.h"
#include "util/statusor.h"

namespace wfa::measurement::common::crypto {

using ::private_join_and_compute::BigNum;
using ::private_join_and_compute::ECGroup;
using ::private_join_and_compute::ECPoint;
// TODO: use absl::StatusOr.
using ::private_join_and_compute::StatusOr;

// Each ElGamalCiphertext is a two tuple (u, e), where u=g^r and e=m*y^r.
using ElGamalCiphertext = std::pair<std::string, std::string>;

// A struture containing the two ECPoints of an ElGamal encryption.
struct ElGamalEcPointPair {
  ECPoint u;  // g^r
  ECPoint e;  // m*y^r
};

// Gets the equivalent ECPoint depiction of a ElGamalCiphertext
StatusOr<ElGamalEcPointPair> GetElGamalEcPoints(
    const ElGamalCiphertext& cipher_text, const ECGroup& ec_group);

// Computes the sum of two ElGamalEcPointPairs.
StatusOr<ElGamalEcPointPair> AddEcPointPairs(const ElGamalEcPointPair& a,
                                             const ElGamalEcPointPair& b);

// Computes the inverse of an ElGamalEcPointPair
StatusOr<ElGamalEcPointPair> InvertEcPointPair(
    const ElGamalEcPointPair& ec_point_pair);

// Computes the multiplication of an ElGamalEcPointPair and scalar
StatusOr<ElGamalEcPointPair> MultiplyEcPointPairByScalar(
    const ElGamalEcPointPair& ec_point_pair, const BigNum& n);

}  // namespace wfa::measurement::common::crypto

#endif  // WFA_MEASUREMENT_COMMON_CRYPTO_EC_POINT_UTIL_H_
