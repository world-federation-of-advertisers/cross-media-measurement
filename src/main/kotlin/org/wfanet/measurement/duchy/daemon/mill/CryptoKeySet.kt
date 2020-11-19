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

package org.wfanet.measurement.duchy.daemon.mill

import org.wfanet.measurement.common.crypto.ElGamalKeyPair
import org.wfanet.measurement.common.crypto.ElGamalPublicKey
import org.wfanet.measurement.common.hexAsByteString

/**
 * All crypto keys necessary for the computations in the mill.
 */
data class CryptoKeySet(
  // The public and private ElGamal keys of the duchy that owns the mill.
  val ownPublicAndPrivateKeys: ElGamalKeyPair,
  // A map from other duchies' name and their public ElGamal keys.
  val otherDuchyPublicKeys: Map<String, ElGamalPublicKey>,
  // The client ElGamal public keys combined from all duchies' public keys.
  val clientPublicKey: ElGamalPublicKey,
  // The id of the elliptic curve
  val curveId: Int
)

// The bytes of keys in the private_join_and_compute crypto library.
const val BYTES_PER_PUBLIC_KEY = 33
const val BYTES_PER_PRIVATE_KEY = 32
const val BYTES_OF_EL_GAMAL_PUBLIC_KEYS = BYTES_PER_PUBLIC_KEY * 2
const val BYTES_OF_EL_GAMAL_KEYS = BYTES_OF_EL_GAMAL_PUBLIC_KEYS + BYTES_PER_PRIVATE_KEY

/**
 * Convert a hexString to its equivalent ElGamalKeyPair proto object.
 */
fun String.toElGamalKeyPair(): ElGamalKeyPair {
  require(length == BYTES_OF_EL_GAMAL_KEYS * 2) {
    "Expected string size : ${BYTES_OF_EL_GAMAL_KEYS * 2}, actual size $length."
  }
  return ElGamalKeyPair.newBuilder()
    .setPublicKey(substring(0, BYTES_OF_EL_GAMAL_PUBLIC_KEYS * 2).toElGamalPublicKey())
    .setSecretKey(substring(BYTES_OF_EL_GAMAL_PUBLIC_KEYS * 2).hexAsByteString())
    .build()
}

/**
 * Convert a hexString to its equivalent ElGamalPublicKey proto object.
 */
fun String.toElGamalPublicKey(): ElGamalPublicKey {
  require(length == BYTES_OF_EL_GAMAL_PUBLIC_KEYS * 2) {
    "Expected string size : ${BYTES_OF_EL_GAMAL_PUBLIC_KEYS * 2}, actual size $length."
  }
  return ElGamalPublicKey.newBuilder()
    .setGenerator(substring(0, BYTES_PER_PUBLIC_KEY * 2).hexAsByteString())
    .setElement(substring(BYTES_PER_PUBLIC_KEY * 2).hexAsByteString())
    .build()
}
