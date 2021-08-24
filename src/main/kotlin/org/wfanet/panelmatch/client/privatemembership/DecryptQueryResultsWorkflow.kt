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

import com.google.protobuf.ByteString
import java.io.Serializable
import org.apache.beam.sdk.values.PCollection
import org.wfanet.panelmatch.common.beam.parDo

/**
 * Implements a query decryption engine in Apache Beam that decrypts a query result
 *
 * @param parameters tuning knobs for the workflow
 * @param privateMembershipCryptor implementation of lower-level oblivious query expansion and
 * result decryption
 */
class DecryptQueryResultsWorkflow(
  private val obliviousQueryParameters: Parameters,
  private val privateMembershipCryptor: PrivateMembershipCryptor
) : Serializable {

  /** Tuning knobs for the [CreateQueriesWorkflow]. */
  data class Parameters(
    val obliviousQueryParameters: ObliviousQueryParameters,
    val publicKey: ByteString,
    val privateKey: ByteString
  ) : Serializable

  /** Creates [EncryptQueriesResponse] on [data]. TODO remove AES encryption */
  fun batchDecryptQueryResults(
    encryptedQueryResults: PCollection<EncryptedQueryResult>
  ): PCollection<DecryptedQueryResult> {
    return encryptedQueryResults.parDo(name = "Decrypt encrypted results") { encryptedQueryResult ->
      val decryptQueryResultsRequest = decryptQueriesRequest {
        parameters = obliviousQueryParameters.obliviousQueryParameters
        publicKey = obliviousQueryParameters.publicKey
        privateKey = obliviousQueryParameters.privateKey
        this.encryptedQueryResults += encryptedQueryResult
      }
      val decryptedResults =
        privateMembershipCryptor.decryptQueryResults(decryptQueryResultsRequest)
      yieldAll(decryptedResults.decryptedQueryResultsList)
    }
  }
}
