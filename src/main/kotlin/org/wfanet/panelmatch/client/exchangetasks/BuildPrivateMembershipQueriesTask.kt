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
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionView
import org.wfanet.panelmatch.client.logger.addToTaskLog
import org.wfanet.panelmatch.client.logger.loggerFor
import org.wfanet.panelmatch.client.privatemembership.CreateQueriesParameters
import org.wfanet.panelmatch.client.privatemembership.EncryptedQueryBundle
import org.wfanet.panelmatch.client.privatemembership.PanelistKeyAndJoinKey
import org.wfanet.panelmatch.client.privatemembership.PrivateMembershipCryptor
import org.wfanet.panelmatch.client.privatemembership.QueryIdAndPanelistKey
import org.wfanet.panelmatch.client.privatemembership.createQueries
import org.wfanet.panelmatch.client.storage.VerifiedStorageClient.VerifiedBlob
import org.wfanet.panelmatch.common.ShardedFileName
import org.wfanet.panelmatch.common.beam.map
import org.wfanet.panelmatch.common.beam.mapWithSideInput
import org.wfanet.panelmatch.common.beam.toSingletonView
import org.wfanet.panelmatch.common.crypto.AsymmetricKeys
import org.wfanet.panelmatch.common.toByteString

class BuildPrivateMembershipQueriesTask(
  override val localCertificate: X509Certificate,
  override val uriPrefix: String,
  override val privateKey: PrivateKey,
  private val parameters: CreateQueriesParameters,
  private val privateMembershipCryptor: PrivateMembershipCryptor,
  private val outputs: Outputs
) : ApacheBeamTask() {

  data class Outputs(
    val encryptedQueriesFileName: String,
    val encryptedQueriesFileCount: Int,
    val queryIdAndPanelistKeyFileName: String,
    val queryIdAndPanelistKeyFileCount: Int
  )

  override suspend fun execute(input: Map<String, VerifiedBlob>): Map<String, Flow<ByteString>> {
    logger.addToTaskLog("Executing build private membership queries")
    val pipeline = Pipeline.create()

    // TODO: previous steps need to output in this format.
    // TODO: need to update all file names to translate labels via step.inputsMap/outputsMap
    val panelistKeyAndJoinKeysManifest = input.getValue("panelists-and-joinkeys")
    val panelistKeyAndJoinKeys =
      readFromManifest(panelistKeyAndJoinKeysManifest, localCertificate).map {
        PanelistKeyAndJoinKey.parseFrom(it)
      }

    val privateKeys =
      readFileAsSingletonPCollection(
        input.getValue("rlwe-serialized-private-key").toStringUtf8(),
        localCertificate
      )
    val publicKeyView =
      readFileAsSingletonPCollection(
          input.getValue("rlwe-serialized-public-key").toStringUtf8(),
          localCertificate
        )
        .toSingletonView()
    val privateMembershipKeys: PCollectionView<AsymmetricKeys> =
      privateKeys
        .mapWithSideInput(publicKeyView, "Make Private Membership Keys") { privateKey, publicKey ->
          AsymmetricKeys(serializedPublicKey = publicKey, serializedPrivateKey = privateKey)
        }
        .toSingletonView()

    val (
      queryIdAndPanelistKeys: PCollection<QueryIdAndPanelistKey>,
      encryptedResponses: PCollection<EncryptedQueryBundle>) =
      createQueries(
        panelistKeyAndJoinKeys,
        privateMembershipKeys,
        parameters,
        privateMembershipCryptor
      )

    val queryDecryptionKeysFileSpec =
      ShardedFileName(outputs.queryIdAndPanelistKeyFileName, outputs.queryIdAndPanelistKeyFileCount)
    require(queryDecryptionKeysFileSpec.shardCount == outputs.queryIdAndPanelistKeyFileCount)
    queryIdAndPanelistKeys.map { it.toByteString() }.write(queryDecryptionKeysFileSpec)

    val encryptedQueriesFileSpec =
      ShardedFileName(outputs.encryptedQueriesFileName, outputs.encryptedQueriesFileCount)
    require(encryptedQueriesFileSpec.shardCount == outputs.encryptedQueriesFileCount)
    encryptedResponses.map { it.toByteString() }.write(encryptedQueriesFileSpec)

    pipeline.run()

    return mapOf(
      "query-decryption-keys" to flowOf(queryDecryptionKeysFileSpec.spec.toByteString()),
      "encrypted-queries" to flowOf(encryptedQueriesFileSpec.spec.toByteString())
    )
  }

  companion object {
    private val logger by loggerFor()
  }
}
