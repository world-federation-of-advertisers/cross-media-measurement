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

package org.wfanet.measurement.duchy.mill

import com.google.protobuf.ByteString
import org.wfanet.measurement.crypto.ElGamalKeyPair
import org.wfanet.measurement.crypto.ElGamalPublicKey
import org.wfanet.measurement.internal.duchy.ElGamalKeys
import org.wfanet.measurement.internal.duchy.ElGamalPublicKeys

/**
 * All crypto keys necessary for the computations in the mill.
 */
data class CryptoKeySet(
  // The public and private ElGamal keys of the duchy that owns the mill.
  val ownPublicAndPrivateKeys: ElGamalKeys,
  // A map from other duchies' name and their public ElGamal keys.
  val otherDuchyPublicKeys: Map<String, ElGamalPublicKeys>,
  // The client ElGamal public keys combined from all duchies' public keys.
  val clientPublicKey: ElGamalPublicKeys,
  // The id of the elliptic curve
  val curveId: Int
)

// The bytes of keys in the private_join_and_compute crypto library.
const val BYTES_PER_PUBLIC_KEY = 33
const val BYTES_PER_PRIVATE_KEY = 32
const val BYTES_OF_EL_GAMAL_PUBLIC_KEYS = BYTES_PER_PUBLIC_KEY * 2
const val BYTES_OF_EL_GAMAL_KEYS = BYTES_OF_EL_GAMAL_PUBLIC_KEYS + BYTES_PER_PRIVATE_KEY

/**
 * Convert a hexString to its equivalent ElGamalKeys proto object.
 */
fun String.toElGamalKeys(): ElGamalKeys {
  require(length == BYTES_OF_EL_GAMAL_KEYS * 2) {
    "Expected string size : ${BYTES_OF_EL_GAMAL_KEYS * 2}, actual size $length."
  }
  return ElGamalKeys.newBuilder()
    .setElGamalPk(substring(0, BYTES_OF_EL_GAMAL_PUBLIC_KEYS * 2).toElGamalPublicKeys())
    .setElGamalSk(hexToByteString(substring(BYTES_OF_EL_GAMAL_PUBLIC_KEYS * 2)))
    .build()
}

/**
 * Convert a hexString to its equivalent ElGamalPublicKeys proto object.
 */
fun String.toElGamalPublicKeys(): ElGamalPublicKeys {
  require(length == BYTES_OF_EL_GAMAL_PUBLIC_KEYS * 2) {
    "Expected string size : ${BYTES_OF_EL_GAMAL_PUBLIC_KEYS * 2}, actual size $length."
  }
  return ElGamalPublicKeys.newBuilder()
    .setElGamalG(hexToByteString(substring(0, BYTES_PER_PUBLIC_KEY * 2)))
    .setElGamalY(hexToByteString(substring(BYTES_PER_PUBLIC_KEY * 2)))
    .build()
}

/**
 * Convert a hexString to its equivalent ByteString.
 * Every two hex numbers in the hexString is mapping to a Byte in the ByteString.
 */
private fun hexToByteString(hexString: String): ByteString? {
  require(hexString.length % 2 == 0)
  val result = ByteArray(hexString.length / 2)
  for (i in result.indices) {
    val decimal = hexString.substring(i * 2, i * 2 + 2).toInt(16)
    result[i] = decimal.toByte()
  }
  return ByteString.copyFrom(result)
}

fun ElGamalPublicKey.toProtoMessage(): ElGamalPublicKeys {
  return ElGamalPublicKeys.newBuilder().apply {
    elGamalG = generator
    elGamalY = element
  }.build()
}

fun ElGamalKeyPair.toProtoMessage(): ElGamalKeys {
  return ElGamalKeys.newBuilder().apply {
    elGamalPk = publicKey.toProtoMessage()
    elGamalSk = secretKey
  }.build()
}
