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

package org.wfanet.measurement.crypto

import com.google.protobuf.ByteString
import java.nio.ByteBuffer
import org.wfanet.measurement.common.size
import org.wfanet.measurement.common.toByteArray

/** Bytes per elliptic curve point. */
private const val BYTES_PER_EC_POINT = 33

/**
 * An elliptic curve ElGamal public key.
 *
 * The generator and element are binary encodings of an OpenSSL `EC_POINT`.
 *
 * @param ellipticCurveId the ID of the OpenSSL built-in elliptic curve
 * @param generator the generator `g` of the public key
 * @param element an element `g^x` where `x` is the secret key
 */
data class ElGamalPublicKey(
  val ellipticCurveId: Int,
  val generator: ByteString,
  val element: ByteString
) {
  init {
    require(generator.size == GENERATOR_SIZE) {
      "Expected $GENERATOR_SIZE bytes for generator. Got ${generator.size}."
    }
    require(element.size == ELEMENT_SIZE) {
      "Expected $ELEMENT_SIZE bytes for element. Got ${element.size}."
    }
  }

  /** Returns the concatenation of [generator] and [element]. */
  fun toByteArray(): ByteArray = listOf(generator, element).toByteArray()

  companion object {
    /** The size of a [generator]. */
    const val GENERATOR_SIZE = BYTES_PER_EC_POINT

    /** The size of an [element]. */
    const val ELEMENT_SIZE = BYTES_PER_EC_POINT

    /**
     * Constructs an [ElGamalPublicKey] from the generator and element as a
     * concatenated byte array.
     */
    fun fromByteArray(bytes: ByteArray, ellipticCurveId: Int): ElGamalPublicKey {
      val buffer = ByteBuffer.wrap(bytes)

      return ElGamalPublicKey(
        ellipticCurveId,
        ByteString.copyFrom(buffer, BYTES_PER_EC_POINT),
        ByteString.copyFrom(buffer, BYTES_PER_EC_POINT)
      )
    }

    fun combine(vararg publicKeys: ElGamalPublicKey): ElGamalPublicKey {
      require(publicKeys.isNotEmpty())
      return combine(publicKeys.asSequence())
    }

    fun combine(publicKeys: Sequence<ElGamalPublicKey>): ElGamalPublicKey {
      val (ellipticCurveId, generator, _) = publicKeys.first()

      for (publicKey in publicKeys.drop(1)) {
        require(publicKey.ellipticCurveId == ellipticCurveId && publicKey.generator == generator) {
          "Cannot combine public keys with different curve IDs or generators."
        }
      }

      // TODO(wangyaopw): Expose ECPoint addition to JVM code so this can be implemented.
      TODO()
    }
  }
}
