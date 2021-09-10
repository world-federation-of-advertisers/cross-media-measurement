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
import com.google.privatemembership.batch.client.Client.EncryptQueriesResponse as ClientEncryptQueriesResponse
import com.google.privatemembership.batch.client.Client.GenerateKeysResponse as ClientGenerateKeysResponse
import com.google.privatemembership.batch.client.Client.PrivateKey as ClientPrivateKey
import com.google.privatemembership.batch.client.encryptQueriesRequest as clientEncryptQueriesRequest
import com.google.privatemembership.batch.client.generateKeysRequest as clientGenerateKeysRequest
import com.google.privatemembership.batch.client.plaintextQuery as clientPlaintextQuery
import com.google.privatemembership.batch.queryMetadata as clientQueryMetadata
import java.nio.file.Paths
import org.wfanet.panelmatch.common.loadLibrary
import org.wfanet.panelmatch.common.wrapJniException
import org.wfanet.panelmatch.protocol.privatemembership.PrivateMembershipWrapper

/** A [PrivateMembershipCryptor] implementation using the JNI [PrivateMembershipWrapper]. */
class JniPrivateMembershipCryptor : PrivateMembershipCryptor {

  override fun generateKeys(request: GenerateKeysRequest): GenerateKeysResponse {
    val clientRequest = clientGenerateKeysRequest {
      parameters = ClientParameters.parseFrom(request.serializedParameters)
    }
    val keys = wrapJniException {
      ClientGenerateKeysResponse.parseFrom(
        PrivateMembershipWrapper.generateKeysWrapper(clientRequest.toByteArray())
      )
    }
    return generateKeysResponse {
      serializedPrivateKey = keys.privateKey.toByteString()
      serializedPublicKey = keys.publicKey.toByteString()
    }
  }

  override fun encryptQueries(
    request: PrivateMembershipEncryptRequest
  ): PrivateMembershipEncryptResponse {
    val plaintextQueries =
      request.unencryptedQueriesList.map {
        clientPlaintextQuery {
          bucketId = it.bucketId.id
          queryMetadata =
            clientQueryMetadata {
              queryId = it.queryId.id
              shardId = it.shardId.id
            }
        }
      }
    val clientRequest = clientEncryptQueriesRequest {
      parameters = ClientParameters.parseFrom(request.serializedParameters)
      privateKey = ClientPrivateKey.parseFrom(request.serializedPrivateKey)
      publicKey = ClientPublicKey.parseFrom(request.serializedPublicKey)
      this.plaintextQueries += plaintextQueries
    }
    val clientResponse = wrapJniException {
      ClientEncryptQueriesResponse.parseFrom(
        PrivateMembershipWrapper.encryptQueriesWrapper(clientRequest.toByteArray())
      )
    }
    val queryMetadata = clientResponse.encryptedQueries.queryMetadataList
    val ciphertexts = clientResponse.encryptedQueries.encryptedQueriesList.map { it.toByteString() }
    return privateMembershipEncryptResponse {
      metadata = clientResponse.encryptedQueries.prngSeed
      this.ciphertexts += ciphertexts
      encryptedQuery +=
        queryMetadata.map {
          encryptedQueryOf(shardId = shardIdOf(it.shardId), queryId = queryIdOf(it.queryId))
        }
    }
  }

  companion object {
    private val SWIG_PATH =
      "panel_exchange_client/src/main/swig/wfanet/panelmatch/client/privatemembership/querybuilder"
    init {
      loadLibrary(name = "private_membership", directoryPath = Paths.get(SWIG_PATH))
    }
  }
}
