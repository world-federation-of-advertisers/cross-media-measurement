// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.panelmatch.client.privatemembership.testing

import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import org.wfanet.panelmatch.client.privatemembership.PrivateMembershipCryptor
import org.wfanet.panelmatch.client.privatemembership.UnencryptedQuery
import org.wfanet.panelmatch.common.crypto.AsymmetricKeyPair

/**
 * Fake [PlaintextPrivateMembershipCryptor] for testing purposes.
 *
 * Built to be compatible with the [PlaintextQueryEvaluator].
 */
class PlaintextPrivateMembershipCryptor : PrivateMembershipCryptor {
  private val privateMembershipCryptorHelper = PlaintextPrivateMembershipCryptorHelper()

  override fun generateKeys(): AsymmetricKeyPair {
    return AsymmetricKeyPair(
      serializedPublicKey = "some public key".toByteStringUtf8(),
      serializedPrivateKey = "some private key".toByteStringUtf8(),
    )
  }

  /**
   * Creates a fake set of ciphertexts where each ciphertext is just the serialized query bundles
   * for each shard
   */
  override fun encryptQueries(
    unencryptedQueries: Iterable<UnencryptedQuery>,
    keys: AsymmetricKeyPair,
  ): ByteString {
    val shardId = unencryptedQueries.first().shardId
    unencryptedQueries.forEach {
      require(it.shardId == shardId) { "All queries must be from the same shard" }
    }
    return privateMembershipCryptorHelper
      .makeEncryptedQueryBundle(
        shard = shardId,
        queries = unencryptedQueries.map { it.queryId to it.bucketId },
      )
      .toByteString()
  }
}
