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
import org.wfanet.panelmatch.client.common.buildAsPCollectionView
import org.wfanet.panelmatch.client.privatemembership.DecryptedEventDataSet
import org.wfanet.panelmatch.client.privatemembership.EncryptedQueryResult
import org.wfanet.panelmatch.client.privatemembership.QueryResultsDecryptor
import org.wfanet.panelmatch.client.privatemembership.decryptQueryResults
import org.wfanet.panelmatch.client.privatemembership.encryptedQueryResult
import org.wfanet.panelmatch.client.privatemembership.queryIdAndJoinKey
import org.wfanet.panelmatch.client.storage.StorageFactory
import org.wfanet.panelmatch.common.ShardedFileName
import org.wfanet.panelmatch.common.beam.kvOf
import org.wfanet.panelmatch.common.beam.map
import org.wfanet.panelmatch.common.beam.mapWithSideInput
import org.wfanet.panelmatch.common.beam.toSingletonView
import org.wfanet.panelmatch.common.compression.CompressorFactory
import org.wfanet.panelmatch.common.compression.Dictionary
import org.wfanet.panelmatch.common.crypto.AsymmetricKeys
import org.wfanet.panelmatch.common.storage.toByteString
import org.wfanet.panelmatch.common.storage.toStringUtf8
import org.wfanet.panelmatch.common.toByteString

class DecryptPrivateMembershipResultsTask(
  override val storageFactory: StorageFactory,
  private val serializedParameters: ByteString,
  private val queryResultsDecryptor: QueryResultsDecryptor,
  private val compressorFactory: CompressorFactory,
  private val outputs: Outputs
) : ApacheBeamTask() {

  data class Outputs(
    val decryptedEventDataSetFileName: String,
    val decryptedEventDataSetFileCount: Int
  )

  override suspend fun execute(
    input: Map<String, StorageClient.Blob>
  ): Map<String, Flow<ByteString>> {
    val pipeline = Pipeline.create()

    val encryptedQueryResultsFileSpec = input.getValue("encrypted-query-results")
    val encryptedQueryResults: PCollection<EncryptedQueryResult> =
      readFromManifest(encryptedQueryResultsFileSpec, encryptedQueryResult {})

    val queryToJoinKeyFileSpec = input.getValue("query-to-joinkey-map")
    val queryToJoinKey =
      readFromManifest(queryToJoinKeyFileSpec, queryIdAndJoinKey {}).map("To KVs") {
        kvOf(it.queryId, it.joinKey)
      }

    val dictionary =
      readSingleBlobAsPCollection(input.getValue("compression-dictionary").toStringUtf8()).map(
        "Parse as Dictionary"
      ) { Dictionary.parseFrom(it) }

    val compressor = compressorFactory.buildAsPCollectionView(dictionary)

    val hkdfPepper = input.getValue("hkdf-pepper").toByteString()

    val privateKeys =
      readSingleBlobAsPCollection(input.getValue("rlwe-serialized-private-key").toStringUtf8())
    val publicKeyView =
      readSingleBlobAsPCollection(input.getValue("rlwe-serialized-public-key").toStringUtf8())
        .toSingletonView()
    val privateMembershipKeys: PCollectionView<AsymmetricKeys> =
      privateKeys
        .mapWithSideInput(publicKeyView, "Make AsymmetricKeys") { privateKey, publicKey ->
          AsymmetricKeys(serializedPublicKey = publicKey, serializedPrivateKey = privateKey)
        }
        .toSingletonView()

    val decryptedEventDataSet: PCollection<DecryptedEventDataSet> =
      decryptQueryResults(
        encryptedQueryResults,
        queryToJoinKey,
        compressor,
        privateMembershipKeys,
        serializedParameters,
        queryResultsDecryptor,
        hkdfPepper,
      )

    val decryptedEventDataSetFileSpec =
      ShardedFileName(outputs.decryptedEventDataSetFileName, outputs.decryptedEventDataSetFileCount)
    decryptedEventDataSet.write(decryptedEventDataSetFileSpec)

    pipeline.run()

    return mapOf(
      "decrypted-event-data" to flowOf(decryptedEventDataSetFileSpec.spec.toByteString())
    )
  }
}
