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

package org.wfanet.measurement.reporting.service.api.v1alpha

import com.google.common.hash.Hashing.goodFastHash
import com.google.protobuf.ByteString
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.common.crypto.PrivateKeyHandle

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

  private fun fingerprint(key: ByteString) = hashFunction.hashBytes(key.toByteArray()).toString()

  private val principalToKeyPairs: Map<String, Map<String, PrivateKeyHandle>> =
    principalToKeyPairs.mapValues { (_, keyPairs) ->
      keyPairs.associate { (publicKey, privateKey) -> fingerprint(publicKey) to privateKey }
    }

  override suspend fun getPrivateKeyHandle(
    principal: String,
    publicKey: ByteString
  ): PrivateKeyHandle? = principalToKeyPairs[principal]?.get(fingerprint(publicKey))
}
