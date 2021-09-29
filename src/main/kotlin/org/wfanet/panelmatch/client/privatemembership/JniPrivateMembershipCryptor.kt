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
import com.google.privatemembership.batch.client.Client.PrivateKey as ClientPrivateKey
import com.google.privatemembership.batch.client.encryptQueriesRequest
import com.google.privatemembership.batch.client.generateKeysRequest
import com.google.privatemembership.batch.client.plaintextQuery
import com.google.privatemembership.batch.queryMetadata
import com.google.protobuf.ByteString
import org.wfanet.panelmatch.common.loadLibraryFromResource
import org.wfanet.panelmatch.protocol.privatemembership.PrivateMembershipWrapper

/** A [PrivateMembershipCryptor] implementation using the JNI [PrivateMembershipWrapper]. */
class JniPrivateMembershipCryptor(private val serializedParameters: ByteString) :
  PrivateMembershipCryptor {

  override fun generateKeys(): PrivateMembershipKeys {
    val request = generateKeysRequest {
      parameters = ClientParameters.parseFrom(serializedParameters)
    }
    val keys = JniPrivateMembership.generateKeys(request)
    return PrivateMembershipKeys(
      serializedPrivateKey = keys.privateKey.toByteString(),
      serializedPublicKey = keys.publicKey.toByteString(),
    )
  }

  override fun encryptQueries(
    unencryptedQueries: Iterable<UnencryptedQuery>,
    keys: PrivateMembershipKeys,
  ): ByteString {
    val plaintextQueries =
      unencryptedQueries.map {
        plaintextQuery {
          bucketId = it.bucketId.id
          queryMetadata =
            queryMetadata {
              queryId = it.queryId.id
              shardId = it.shardId.id
            }
        }
      }
    val request = encryptQueriesRequest {
      parameters = ClientParameters.parseFrom(serializedParameters)
      privateKey = ClientPrivateKey.parseFrom(keys.serializedPrivateKey)
      publicKey = ClientPublicKey.parseFrom(keys.serializedPublicKey)
      this.plaintextQueries += plaintextQueries
    }
    val response = JniPrivateMembership.encryptQueries(request)
    return response.toByteString()
  }

  companion object {
    init {
      loadLibraryFromResource("private_membership", "$SWIG_PREFIX/privatemembership")
    }
  }
}
