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

package org.wfanet.panelmatch.client.privatemembership.testing

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.client.common.CompressedEvents
import org.wfanet.panelmatch.client.common.EventCompressorTrainer
import org.wfanet.panelmatch.client.eventpreprocessing.compressByKey
import org.wfanet.panelmatch.client.privatemembership.DecryptQueryResultsWorkflow
import org.wfanet.panelmatch.client.privatemembership.DecryptQueryResultsWorkflow.Parameters
import org.wfanet.panelmatch.client.privatemembership.DecryptedEventData
import org.wfanet.panelmatch.client.privatemembership.EncryptedEventData
import org.wfanet.panelmatch.client.privatemembership.EncryptedQueryResult
import org.wfanet.panelmatch.client.privatemembership.JoinKey
import org.wfanet.panelmatch.client.privatemembership.PrivateMembershipCryptor
import org.wfanet.panelmatch.client.privatemembership.QueryId
import org.wfanet.panelmatch.client.privatemembership.QueryResultsDecryptor
import org.wfanet.panelmatch.client.privatemembership.generateKeysRequest
import org.wfanet.panelmatch.client.privatemembership.joinKeyOf
import org.wfanet.panelmatch.client.privatemembership.plaintextOf
import org.wfanet.panelmatch.client.privatemembership.queryIdOf
import org.wfanet.panelmatch.common.beam.keyBy
import org.wfanet.panelmatch.common.beam.kvOf
import org.wfanet.panelmatch.common.beam.map
import org.wfanet.panelmatch.common.beam.mapKeys
import org.wfanet.panelmatch.common.beam.strictOneToOneJoin
import org.wfanet.panelmatch.common.beam.testing.BeamTestBase
import org.wfanet.panelmatch.common.beam.testing.assertThat
import org.wfanet.panelmatch.common.compression.CompressorFactory
import org.wfanet.panelmatch.common.toByteString

private val PLAINTEXTS =
  listOf(
    plaintextOf("<some long data a>", 1, 6),
    plaintextOf("<some long data b>", 2, 7),
    plaintextOf("<some long data c>", 3, 8),
    plaintextOf("<some long data d>", 4, 9),
    plaintextOf("<some long data e>", 5, 10)
  )
private val JOINKEYS =
  listOf(
    Pair(1, "some joinkey 1"),
    Pair(2, "some joinkey 1"),
    Pair(3, "some joinkey 1"),
    Pair(4, "some joinkey 1"),
    Pair(5, "some joinkey 1")
  )
private val HKDF_PEPPER = "some-pepper".toByteString()

@RunWith(JUnit4::class)
abstract class AbstractDecryptQueryResultsWorkflowTest : BeamTestBase() {
  abstract val queryResultsDecryptor: QueryResultsDecryptor
  abstract val privateMembershipCryptor: PrivateMembershipCryptor
  abstract val privateMembershipCryptorHelper: PrivateMembershipCryptorHelper
  abstract val serializedParameters: ByteString
  abstract val eventCompressorTrainer: EventCompressorTrainer
  abstract val compressorFactory: CompressorFactory

  private fun runWorkflow(
    queryResultsDecryptor: QueryResultsDecryptor,
    parameters: Parameters
  ): PCollection<DecryptedEventData> {
    val plaintextCollection: PCollection<DecryptedEventData> =
      pcollectionOf("Create plaintext data", *PLAINTEXTS.toTypedArray())
    val joinkeyCollection: PCollection<KV<QueryId, JoinKey>> =
      pcollectionOf(
        "Create joinkey data",
        *JOINKEYS
          .map { kvOf(queryIdOf(it.first), joinKeyOf(it.second.toByteString())) }
          .toTypedArray()
      )
    val compressedEvents = makeCompressedEvents(plaintextCollection)
    val encryptedResults =
      makeEncryptedResults(joinkeyCollection, plaintextCollection, compressedEvents.events)

    return DecryptQueryResultsWorkflow(
        parameters = parameters,
        queryResultsDecryptor = queryResultsDecryptor,
        hkdfPepper = HKDF_PEPPER,
        compressorFactory = compressorFactory,
      )
      .batchDecryptQueryResults(
        encryptedQueryResults = encryptedResults,
        queryIdToJoinKey = joinkeyCollection,
        dictionary = compressedEvents.dictionary,
      )
  }

  @Test
  fun `Decrypt simple set of results`() {
    val generateKeysRequest = generateKeysRequest {
      this.serializedParameters = serializedParameters
    }
    val generateKeysResponse = privateMembershipCryptor.generateKeys(generateKeysRequest)
    val parameters =
      Parameters(
        serializedParameters = serializedParameters,
        serializedPrivateKey = generateKeysResponse.serializedPrivateKey,
        serializedPublicKey = generateKeysResponse.serializedPublicKey
      )
    val decryptedResults = runWorkflow(queryResultsDecryptor, parameters)
    assertThat(decryptedResults).containsInAnyOrder(PLAINTEXTS)
  }

  private fun makeCompressedEvents(
    plaintextCollection: PCollection<DecryptedEventData>,
  ): CompressedEvents {

    val eventPCollection = plaintextCollection.map { kvOf(it.queryId.toByteString(), it.plaintext) }
    return eventCompressorTrainer.compressByKey(eventPCollection)
  }

  private fun makeEncryptedResults(
    joinkeyCollection: PCollection<KV<QueryId, JoinKey>>,
    plaintextCollection: PCollection<DecryptedEventData>,
    events: PCollection<KV<ByteString, ByteString>>
  ): PCollection<EncryptedQueryResult> {

    val mappedCompressedEvents = events.mapKeys { QueryId.parseFrom(it) }
    val compressedPlaintexts: PCollection<DecryptedEventData> =
      plaintextCollection
        .keyBy<DecryptedEventData, QueryId>("Key by QueryId") { requireNotNull(it.queryId) }
        .strictOneToOneJoin<QueryId, DecryptedEventData, ByteString>(mappedCompressedEvents)
        .map { plaintextOf(it.value, it.key.queryId, it.key.shardId) }

    val encryptedEventData: PCollection<EncryptedEventData> =
      privateMembershipCryptorHelper.makeEncryptedEventData(compressedPlaintexts, joinkeyCollection)
    return privateMembershipCryptorHelper.makeEncryptedQueryResults(encryptedEventData)
  }
}
