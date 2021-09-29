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

package org.wfanet.panelmatch.client.exchangetasks

import com.google.protobuf.ByteString
import java.security.PrivateKey
import java.security.cert.X509Certificate
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.values.PCollection
import org.wfanet.panelmatch.client.privatemembership.CreateQueriesWorkflow
import org.wfanet.panelmatch.client.privatemembership.EncryptedQueryBundle
import org.wfanet.panelmatch.client.privatemembership.JoinKey
import org.wfanet.panelmatch.client.privatemembership.PanelistKey
import org.wfanet.panelmatch.client.privatemembership.PrivateMembershipCryptor
import org.wfanet.panelmatch.client.privatemembership.QueryIdAndPanelistKey
import org.wfanet.panelmatch.client.storage.VerifiedStorageClient
import org.wfanet.panelmatch.common.beam.SignedFiles
import org.wfanet.panelmatch.common.beam.kvOf
import org.wfanet.panelmatch.common.beam.map
import org.wfanet.panelmatch.common.toByteString
import org.wfanet.panelmatch.protocol.SharedInputs

class BuildPrivateMembershipQueriesTask(
  private val numQueries: Int,
  private val parameters: CreateQueriesWorkflow.Parameters,
  private val privateMembershipCryptor: PrivateMembershipCryptor,
  private val localCertificate: X509Certificate,
  private val outputUriPrefix: String,
  private val privateKey: PrivateKey,
  private val encryptedQueryBundleFileCount: Int,
  private val queryIdAndPanelistKeysFileCount: Int,
) : ExchangeTask {
  override suspend fun execute(
    input: Map<String, VerifiedStorageClient.VerifiedBlob>
  ): Map<String, Flow<ByteString>> {
    val pipeline = Pipeline.create()

    val identifiers = SharedInputs.parseFrom(input.getValue("identifiers").toByteString())
    val lookupKeys = SharedInputs.parseFrom(input.getValue("lookup-keys").toByteString())
    val pairs =
      (identifiers.dataList zip lookupKeys.dataList).map {
        kvOf(PanelistKey.parseFrom(it.first), JoinKey.parseFrom(it.second))
      }

    val (
      queryIdAndPanelistKeys: PCollection<QueryIdAndPanelistKey>,
      encryptedResponses: PCollection<EncryptedQueryBundle>) =
      CreateQueriesWorkflow(parameters, privateMembershipCryptor)
        .batchCreateQueries(pipeline.apply(Create.of(pairs)))

    val queryDecryptionKeysFileSpec =
      "$outputUriPrefix/query-decryption-keys-*-of-$queryIdAndPanelistKeysFileCount"
    queryIdAndPanelistKeys
      .map { it.toByteString() }
      .apply(SignedFiles.write(queryDecryptionKeysFileSpec, privateKey, localCertificate))

    val encryptedQueriesFileSpec =
      "$outputUriPrefix/encrypted-queries-*-of-$encryptedQueryBundleFileCount"
    encryptedResponses
      .map { it.toByteString() }
      .apply(SignedFiles.write(encryptedQueriesFileSpec, privateKey, localCertificate))

    pipeline.run()

    return mapOf(
      "query-decryption-keys" to flowOf(queryDecryptionKeysFileSpec.toByteString()),
      "encrypted-queries" to flowOf(encryptedQueriesFileSpec.toByteString())
    )
  }
}
