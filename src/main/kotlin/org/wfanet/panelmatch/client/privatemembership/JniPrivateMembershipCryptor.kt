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

package org.wfanet.panelmatch.client.privatemembership

import com.google.privatemembership.batch.Shared.Parameters as ClientParameters
import com.google.privatemembership.batch.Shared.PublicKey as ClientPublicKey
import com.google.privatemembership.batch.client.Client.EncryptQueriesResponse
import com.google.privatemembership.batch.client.Client.PrivateKey as ClientPrivateKey
import com.google.privatemembership.batch.client.encryptQueriesRequest
import com.google.privatemembership.batch.client.generateKeysRequest
import com.google.privatemembership.batch.client.plaintextQuery
import com.google.privatemembership.batch.queryMetadata
import com.google.protobuf.Any
import com.google.protobuf.ByteString
import org.wfanet.panelmatch.common.crypto.AsymmetricKeyPair
import org.wfanet.panelmatch.protocol.privatemembership.PrivateMembershipSwig

/** A [PrivateMembershipCryptor] implementation using the JNI [PrivateMembershipSwig]. */
class JniPrivateMembershipCryptor(private val parameters: Any) : PrivateMembershipCryptor {

  private val privateMembershipParameters by lazy {
    parameters.unpack(ClientParameters::class.java)
  }

  override fun generateKeys(): AsymmetricKeyPair {
    val request = generateKeysRequest { this.parameters = privateMembershipParameters }
    val keys = JniPrivateMembership.generateKeys(request)
    return AsymmetricKeyPair(
      serializedPrivateKey = keys.privateKey.toByteString(),
      serializedPublicKey = keys.publicKey.toByteString(),
    )
  }

  override fun encryptQueries(
    unencryptedQueries: Iterable<UnencryptedQuery>,
    keys: AsymmetricKeyPair,
  ): ByteString {
    val plaintextQueries =
      unencryptedQueries.map {
        plaintextQuery {
          bucketId = it.bucketId.id
          queryMetadata = queryMetadata {
            queryId = it.queryId.id
            shardId = it.shardId.id
          }
        }
      }
    val request = encryptQueriesRequest {
      this.parameters = privateMembershipParameters
      privateKey = ClientPrivateKey.parseFrom(keys.serializedPrivateKey)
      publicKey = ClientPublicKey.parseFrom(keys.serializedPublicKey)
      this.plaintextQueries += plaintextQueries
    }
    val response: EncryptQueriesResponse = JniPrivateMembership.encryptQueries(request)
    return response.encryptedQueries.toByteString()
  }
}
