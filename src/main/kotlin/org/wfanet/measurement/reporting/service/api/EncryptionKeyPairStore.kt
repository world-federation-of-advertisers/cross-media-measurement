// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.api

import com.google.common.hash.Hashing.goodFastHash
import com.google.protobuf.ByteString
import java.security.GeneralSecurityException
import java.util.logging.Level
import java.util.logging.Logger
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.tink.TinkPublicKeyHandle

private const val DEFAULT_HASH_MINIMUM_BITS = 128

interface EncryptionKeyPairStore {
  /**
   * Retrieves the corresponding [PrivateKeyHandle] for a serialized public key.
   *
   * @param principal resource name the public key belongs to
   * @param publicKey `data` field of an [EncryptionPublicKey]
   */
  suspend fun getPrivateKeyHandle(principal: String, publicKey: ByteString): PrivateKeyHandle?
}

class InMemoryEncryptionKeyPairStore(
  principalToKeyPairs: Map<String, List<Pair<ByteString, PrivateKeyHandle>>>
) : EncryptionKeyPairStore {
  private val hashFunction = goodFastHash(DEFAULT_HASH_MINIMUM_BITS)

  /**
   * Fingerprints [key], the serialized `data` field of an [EncryptionPublicKey], in a
   * Tink-version-stable way.
   *
   * Tink public-key serialization is not guaranteed to be stable across Tink releases, so the same
   * underlying key can have different serialized bytes depending on the Tink version that produced
   * it (e.g. a key read from a checked-in keyset vs. one re-serialized by the running server after
   * a Tink upgrade). Hashing the raw bytes would fail to match those. Parsing and re-serializing
   * with the current Tink before hashing makes any serialization of a given key fingerprint
   * identically.
   */
  private fun fingerprint(key: ByteString): String {
    val normalized: ByteString = TinkPublicKeyHandle(key).toByteString()
    return hashFunction.hashBytes(normalized.toByteArray()).toString()
  }

  // Deliberately eager and unguarded: a malformed entry here is an operator configuration
  // error, not caller input, so it should fail server startup immediately rather than be
  // silently dropped -- contrast with the request path in getPrivateKeyHandle below, which
  // treats the same GeneralSecurityException as recoverable because it comes from a caller.
  private val principalToKeyPairs: Map<String, Map<String, PrivateKeyHandle>> =
    principalToKeyPairs.mapValues { (_, keyPairs) ->
      keyPairs.associate { (publicKey, privateKey) -> fingerprint(publicKey) to privateKey }
    }

  override suspend fun getPrivateKeyHandle(
    principal: String,
    publicKey: ByteString,
  ): PrivateKeyHandle? {
    // A caller-supplied public key that is not a parseable Tink keyset cannot match any stored
    // key, so report it as not found (null) rather than propagating a low-level crypto exception
    // -- unlike the constructor above, this data comes from the request, not from trusted
    // configuration, so failing the whole server is not appropriate.
    val fingerprint =
      try {
        fingerprint(publicKey)
      } catch (e: GeneralSecurityException) {
        logger.log(Level.WARNING, e) {
          "Public key for principal $principal is not a parseable Tink keyset; treating as not " +
            "found"
        }
        return null
      }
    return principalToKeyPairs[principal]?.get(fingerprint)
  }

  companion object {
    private val logger: Logger = Logger.getLogger(InMemoryEncryptionKeyPairStore::class.java.name)
  }
}
