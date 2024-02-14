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

import com.google.protobuf.Any
import org.apache.beam.sdk.values.PCollection
import org.wfanet.panelmatch.client.privatemembership.EncryptedQueryResult
import org.wfanet.panelmatch.client.privatemembership.JoinKeyIdentifierCollection
import org.wfanet.panelmatch.client.privatemembership.KeyedDecryptedEventDataSet
import org.wfanet.panelmatch.client.privatemembership.QueryResultsDecryptor
import org.wfanet.panelmatch.client.privatemembership.decryptQueryResults
import org.wfanet.panelmatch.client.privatemembership.encryptedQueryResult
import org.wfanet.panelmatch.client.privatemembership.queryIdAndId
import org.wfanet.panelmatch.client.privatemembership.removeDiscardedJoinKeys
import org.wfanet.panelmatch.common.beam.flatMap
import org.wfanet.panelmatch.common.beam.map
import org.wfanet.panelmatch.common.beam.mapWithSideInput
import org.wfanet.panelmatch.common.beam.toSingletonView
import org.wfanet.panelmatch.common.compression.CompressionParameters
import org.wfanet.panelmatch.common.crypto.AsymmetricKeyPair

/** Decrypts private membership results given serialized parameters. */
suspend fun ApacheBeamContext.decryptPrivateMembershipResults(
  parameters: Any,
  queryResultsDecryptor: QueryResultsDecryptor,
) {
  val encryptedQueryResults: PCollection<EncryptedQueryResult> =
    readShardedPCollection("encrypted-results", encryptedQueryResult {})

  val queryAndIds = readShardedPCollection("query-to-ids-map", queryIdAndId {})

  // TODO: remove this functionality v2.0.0
  // For backwards compatibility for workflows without discarded-join-keys
  val discardedJoinKeys: List<JoinKeyIdentifier> =
    if ("discarded-join-keys" in inputLabels) {
      JoinKeyIdentifierCollection.parseFrom(readBlob("discarded-join-keys")).joinKeyIdentifiersList
    } else {
      emptyList()
    }
  val plaintextJoinKeyAndIds: PCollection<JoinKeyAndId> =
    readBlobAsPCollection("plaintext-join-keys-to-id-map").flatMap("plaintext-join-keys-id-map") {
      JoinKeyAndIdCollection.parseFrom(it)
        .joinKeyAndIdsList
        .removeDiscardedJoinKeys(discardedJoinKeys)
    }
  val decryptedJoinKeyAndIds: PCollection<JoinKeyAndId> =
    readBlobAsPCollection("decrypted-join-keys-to-id-map").flatMap(
      "decrypted-join-keys-to-id-map"
    ) {
      JoinKeyAndIdCollection.parseFrom(it)
        .joinKeyAndIdsList
        .removeDiscardedJoinKeys(discardedJoinKeys)
    }

  val compressionParameters =
    readBlobAsPCollection("compression-parameters")
      .map("Parse as CompressionParameters") { CompressionParameters.parseFrom(it) }
      .toSingletonView("compression-parameters-singleton-view")

  val hkdfPepper = readBlob("pepper")
  val publicKeyView = readBlobAsView("serialized-rlwe-public-key")

  val privateKeysView =
    readBlobAsPCollection("serialized-rlwe-private-key")
      .mapWithSideInput(publicKeyView, "Make Private Membership Keys") { privateKey, publicKey ->
        AsymmetricKeyPair(serializedPublicKey = publicKey, serializedPrivateKey = privateKey)
      }
      .toSingletonView("rlwe-private-keys-singleton-view")

  val keyedDecryptedEventDataSet: PCollection<KeyedDecryptedEventDataSet> =
    decryptQueryResults(
      encryptedQueryResults,
      plaintextJoinKeyAndIds,
      decryptedJoinKeyAndIds,
      queryAndIds,
      compressionParameters,
      privateKeysView,
      parameters,
      queryResultsDecryptor,
      hkdfPepper,
    )

  keyedDecryptedEventDataSet.writeShardedFiles("decrypted-event-data")
}
