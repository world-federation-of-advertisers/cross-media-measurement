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
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionView
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.logger.addToTaskLog
import org.wfanet.panelmatch.client.logger.loggerFor
import org.wfanet.panelmatch.client.privatemembership.CreateQueriesParameters
import org.wfanet.panelmatch.client.privatemembership.EncryptedQueryBundle
import org.wfanet.panelmatch.client.privatemembership.PrivateMembershipCryptor
import org.wfanet.panelmatch.client.privatemembership.QueryIdAndJoinKeys
import org.wfanet.panelmatch.client.privatemembership.createQueries
import org.wfanet.panelmatch.client.storage.StorageFactory
import org.wfanet.panelmatch.common.ShardedFileName
import org.wfanet.panelmatch.common.beam.mapWithSideInput
import org.wfanet.panelmatch.common.beam.toSingletonView
import org.wfanet.panelmatch.common.crypto.AsymmetricKeys
import org.wfanet.panelmatch.common.storage.toStringUtf8
import org.wfanet.panelmatch.common.toByteString

class BuildPrivateMembershipQueriesTask(
  override val storageFactory: StorageFactory,
  private val parameters: CreateQueriesParameters,
  private val privateMembershipCryptor: PrivateMembershipCryptor,
  private val outputs: Outputs
) : ApacheBeamTask() {

  data class Outputs(
    val encryptedQueryBundlesFileName: String,
    val encryptedQueryBundlesFileCount: Int,
    val queryIdAndJoinKeysFileName: String,
    val queryIdAndJoinKeysFileCount: Int
  )

  override suspend fun execute(
    input: Map<String, StorageClient.Blob>
  ): Map<String, Flow<ByteString>> {
    logger.addToTaskLog("Executing build private membership queries")
    val pipeline = Pipeline.create()

    val lookupKeyAndIdsManifest = input.getValue("lookup-keys")
    val lookupKeyAndIds = readFromManifest(lookupKeyAndIdsManifest, joinKeyAndId {})

    val hashedJoinKeyAndIdsManifest = input.getValue("hashed-join-keys")
    val hashedJoinKeyAndIds = readFromManifest(hashedJoinKeyAndIdsManifest, joinKeyAndId {})

    val privateKeys =
      readSingleBlobAsPCollection(input.getValue("rlwe-serialized-private-key").toStringUtf8())
    val publicKeyView =
      readSingleBlobAsPCollection(input.getValue("rlwe-serialized-public-key").toStringUtf8())
        .toSingletonView()
    val privateMembershipKeys: PCollectionView<AsymmetricKeys> =
      privateKeys
        .mapWithSideInput(publicKeyView, "Make Private Membership Keys") { privateKey, publicKey ->
          AsymmetricKeys(serializedPublicKey = publicKey, serializedPrivateKey = privateKey)
        }
        .toSingletonView()

    val (
      queryIdAndJoinKeys: PCollection<QueryIdAndJoinKeys>,
      encryptedQueryBundles: PCollection<EncryptedQueryBundle>) =
      createQueries(
        lookupKeyAndIds,
        hashedJoinKeyAndIds,
        privateMembershipKeys,
        parameters,
        privateMembershipCryptor
      )

    val queryIdAndJoinKeysFileSpec =
      ShardedFileName(outputs.queryIdAndJoinKeysFileName, outputs.queryIdAndJoinKeysFileCount)
    queryIdAndJoinKeys.write(queryIdAndJoinKeysFileSpec)

    val encryptedQueryBundlesFileSpec =
      ShardedFileName(outputs.encryptedQueryBundlesFileName, outputs.encryptedQueryBundlesFileCount)
    encryptedQueryBundles.write(encryptedQueryBundlesFileSpec)

    pipeline.run()

    return mapOf(
      "query-to-join-keys-map" to flowOf(queryIdAndJoinKeysFileSpec.spec.toByteString()),
      "encrypted-queries" to flowOf(encryptedQueryBundlesFileSpec.spec.toByteString())
    )
  }

  companion object {
    private val logger by loggerFor()
  }
}
