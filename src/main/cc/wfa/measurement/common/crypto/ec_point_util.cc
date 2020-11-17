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

#include "wfa/measurement/common/crypto/ec_point_util.h"

#include "absl/status/statusor.h"

namespace wfa::measurement::common::crypto {

absl::StatusOr<ElGamalEcPointPair> GetElGamalEcPoints(
    const ElGamalCiphertext& cipher_text, const ECGroup& ec_group) {
  ASSIGN_OR_RETURN(ECPoint ec_point_u,
                   ec_group.CreateECPoint(cipher_text.first));
  ASSIGN_OR_RETURN(ECPoint ec_point_e,
                   ec_group.CreateECPoint(cipher_text.second));
  ElGamalEcPointPair result = {
      .u = std::move(ec_point_u),
      .e = std::move(ec_point_e),
  };
  return result;
}

absl::StatusOr<ElGamalEcPointPair> AddEcPointPairs(
    const ElGamalEcPointPair& a, const ElGamalEcPointPair& b) {
  ASSIGN_OR_RETURN(ECPoint result_u, a.u.Add(b.u));
  ASSIGN_OR_RETURN(ECPoint result_e, a.e.Add(b.e));
  ElGamalEcPointPair result = {
      .u = std::move(result_u),
      .e = std::move(result_e),
  };
  return result;
}

absl::StatusOr<ElGamalEcPointPair> InvertEcPointPair(
    const ElGamalEcPointPair& ec_point_pair) {
  ASSIGN_OR_RETURN(ECPoint inverse_u, ec_point_pair.u.Inverse());
  ASSIGN_OR_RETURN(ECPoint inverse_e, ec_point_pair.e.Inverse());
  ElGamalEcPointPair result = {
      .u = std::move(inverse_u),
      .e = std::move(inverse_e),
  };
  return result;
}

absl::StatusOr<ElGamalEcPointPair> MultiplyEcPointPairByScalar(
    const ElGamalEcPointPair& ec_point_pair, const BigNum& n) {
  ASSIGN_OR_RETURN(ECPoint result_u, ec_point_pair.u.Mul(n));
  ASSIGN_OR_RETURN(ECPoint result_e, ec_point_pair.e.Mul(n));
  ElGamalEcPointPair result = {
      .u = std::move(result_u),
      .e = std::move(result_e),
  };
  return result;
}

}  // namespace wfa::measurement::common::crypto
