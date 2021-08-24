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

import com.google.privatemembership.batch.Shared.EncryptedQueryResult as ClientEncryptedQueryResult
import com.google.privatemembership.batch.Shared.Parameters as ClientParameters
import com.google.privatemembership.batch.Shared.PublicKey as ClientPublicKey
import com.google.privatemembership.batch.client.Client.DecryptQueriesResponse as ClientDecryptQueriesResponse
import com.google.privatemembership.batch.client.Client.EncryptQueriesResponse as ClientEncryptQueriesResponse
import com.google.privatemembership.batch.client.Client.GenerateKeysResponse as ClientGenerateKeysResponse
import com.google.privatemembership.batch.client.Client.PrivateKey as ClientPrivateKey
import com.google.privatemembership.batch.client.decryptQueriesRequest as clientDecryptQueriesRequest
import com.google.privatemembership.batch.client.encryptQueriesRequest as clientEncryptQueriesRequest
import com.google.privatemembership.batch.client.generateKeysRequest as clientGenerateKeysRequest
import com.google.privatemembership.batch.client.plaintextQuery as clientPlaintextQuery
import com.google.privatemembership.batch.encryptedQueryResult as clientEncryptedQueryResult
import com.google.privatemembership.batch.queryMetadata as clientQueryMetadata
import java.nio.file.Paths
import org.wfanet.panelmatch.common.loadLibrary
import org.wfanet.panelmatch.common.wrapJniException
import org.wfanet.panelmatch.protocol.privatemembership.ObliviousQueryWrapper
import rlwe.Serialization.SerializedSymmetricRlweCiphertext

/** A [PrivateMembershipCryptor] implementation using the JNI [ObliviousQueryWrapper]. */
class JniPrivateMembershipCryptor(private val clientParameters: ClientParameters) :
  PrivateMembershipCryptor {
  private val clientPrivateKey: ClientPrivateKey
  private val clientPublicKey: ClientPublicKey
  init {
    val keys = generateKeys(generateKeysRequest {})
    clientPublicKey = ClientPublicKey.parseFrom(keys.publicKey)
    clientPrivateKey = ClientPrivateKey.parseFrom(keys.privateKey)
  }

  override fun generateKeys(request: GenerateKeysRequest): GenerateKeysResponse {
    val clientRequest = clientGenerateKeysRequest { parameters = clientParameters }
    val keys = wrapJniException {
      ClientGenerateKeysResponse.parseFrom(
        ObliviousQueryWrapper.generateKeysWrapper(clientRequest.toByteArray())
      )
    }
    return generateKeysResponse {
      privateKey = keys.privateKey.toByteString()
      publicKey = keys.publicKey.toByteString()
    }
  }

  override fun encryptQueries(request: EncryptQueriesRequest): EncryptQueriesResponse {
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
      parameters = clientParameters
      privateKey = clientPrivateKey
      publicKey = clientPublicKey
      this.plaintextQueries += plaintextQueries
    }
    val clientResponse = wrapJniException {
      ClientEncryptQueriesResponse.parseFrom(
        ObliviousQueryWrapper.encryptQueriesWrapper(clientRequest.toByteArray())
      )
    }
    val queryMetadata = clientResponse.encryptedQueries.queryMetadataList
    val ciphertexts = clientResponse.encryptedQueries.encryptedQueriesList.map { it.toByteString() }
    return encryptQueriesResponse {
      metadata = clientResponse.encryptedQueries.prngSeed
      this.ciphertexts += ciphertexts
      this.encryptedQuery +=
        queryMetadata.map { encryptedQueryOf(shard = it.shardId, query = it.queryId) }
    }
  }

  override fun decryptQueryResults(request: DecryptQueriesRequest): DecryptQueriesResponse {
    val encryptedQueries: List<ClientEncryptedQueryResult> =
      request.encryptedQueryResultsList.map { encryptedResult ->
        clientEncryptedQueryResult {
          ciphertexts +=
            encryptedResult.ciphertextsList.map { ciphertext ->
              SerializedSymmetricRlweCiphertext.parseFrom(ciphertext)
            }
        }
      }
    val clientRequest = clientDecryptQueriesRequest {
      parameters = clientParameters
      privateKey = privateKey
      publicKey = publicKey
      this.encryptedQueries += encryptedQueries
    }
    val clientResponse = wrapJniException {
      ClientDecryptQueriesResponse.parseFrom(
        ObliviousQueryWrapper.decryptQueriesWrapper(clientRequest.toByteArray())
      )
    }
    val mappedResults =
      clientResponse.resultList.map { result ->
        plaintextOf(
          shard = result.queryMetadata.shardId,
          query = result.queryMetadata.queryId,
          plaintext = result.result
        )
      }
    return decryptQueriesResponse { decryptedQueryResults += mappedResults }
  }

  companion object {
    private val SWIG_PATH =
      "panel_exchange_client/src/main/swig/wfanet/panelmatch/client/privatemembership/querybuilder"
    init {
      loadLibrary(name = "oblivious_query", directoryPath = Paths.get(SWIG_PATH))
    }
  }
}
