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
import org.wfanet.panelmatch.client.privatemembership.DecryptEventDataRequest.EncryptedEventDataSet
import org.wfanet.panelmatch.client.privatemembership.DecryptEventDataRequestKt.encryptedEventDataSet
import org.wfanet.panelmatch.client.privatemembership.DecryptQueryResultsWorkflow
import org.wfanet.panelmatch.client.privatemembership.DecryptQueryResultsWorkflow.Parameters
import org.wfanet.panelmatch.client.privatemembership.DecryptedEventDataSet
import org.wfanet.panelmatch.client.privatemembership.EncryptedQueryResult
import org.wfanet.panelmatch.client.privatemembership.JoinKey
import org.wfanet.panelmatch.client.privatemembership.Plaintext
import org.wfanet.panelmatch.client.privatemembership.PrivateMembershipCryptor
import org.wfanet.panelmatch.client.privatemembership.PrivateMembershipKeys
import org.wfanet.panelmatch.client.privatemembership.QueryId
import org.wfanet.panelmatch.client.privatemembership.QueryResultsDecryptor
import org.wfanet.panelmatch.client.privatemembership.decryptedEventDataSet
import org.wfanet.panelmatch.client.privatemembership.joinKeyOf
import org.wfanet.panelmatch.client.privatemembership.plaintext
import org.wfanet.panelmatch.client.privatemembership.plaintextOf
import org.wfanet.panelmatch.client.privatemembership.queryIdOf
import org.wfanet.panelmatch.common.beam.groupByKey
import org.wfanet.panelmatch.common.beam.kvOf
import org.wfanet.panelmatch.common.beam.map
import org.wfanet.panelmatch.common.beam.parDo
import org.wfanet.panelmatch.common.beam.strictOneToOneJoin
import org.wfanet.panelmatch.common.beam.testing.BeamTestBase
import org.wfanet.panelmatch.common.beam.testing.assertThat
import org.wfanet.panelmatch.common.compression.CompressorFactory
import org.wfanet.panelmatch.common.toByteString

private val PLAINTEXTS: List<Pair<Int, List<Plaintext>>> =
  listOf(
    1 to listOf(plaintextOf("<some long data a>"), plaintextOf("<some long data b>")),
    2 to listOf(plaintextOf("<some long data c>"), plaintextOf("<some long data d>")),
    3 to listOf(plaintextOf("<some long data e>"))
  )
private val JOINKEYS: List<Pair<Int, String>> =
  listOf(
    1 to "some joinkey 1",
    2 to "some joinkey 2",
    3 to "some joinkey 3",
  )
private val HKDF_PEPPER = "some-pepper".toByteString()

@RunWith(JUnit4::class)
abstract class AbstractDecryptQueryResultsWorkflowTest : BeamTestBase() {
  abstract val queryResultsDecryptor: QueryResultsDecryptor
  abstract val privateMembershipCryptor: PrivateMembershipCryptor
  abstract val privateMembershipCryptorHelper: PrivateMembershipCryptorHelper
  abstract val privateMembershipSerializedParameters: ByteString
  abstract val eventCompressorTrainer: EventCompressorTrainer
  abstract val compressorFactory: CompressorFactory

  private fun runWorkflow(
    queryResultsDecryptor: QueryResultsDecryptor,
    parameters: Parameters
  ): PCollection<DecryptedEventDataSet> {
    val keys =
      PrivateMembershipKeys(
        serializedPrivateKey = parameters.serializedPrivateKey,
        serializedPublicKey = parameters.serializedPublicKey
      )
    val plaintextCollection: PCollection<DecryptedEventDataSet> =
      pcollectionOf(
        "Create plaintext data",
        PLAINTEXTS.map {
          decryptedEventDataSet {
            queryId = queryIdOf(it.first)
            decryptedEventData += it.second
          }
        }
      )
    val compressedEvents = makeCompressedEvents(plaintextCollection)
    val joinkeyCollection: PCollection<KV<QueryId, JoinKey>> =
      pcollectionOf(
        "Create joinkey data",
        JOINKEYS.map { kvOf(queryIdOf(it.first), joinKeyOf(it.second)) }
      )
    val encryptedResults =
      makeEncryptedResults(
        privateMembershipCryptorHelper,
        keys,
        joinkeyCollection,
        compressedEvents.events
      )

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
    val keys = privateMembershipCryptor.generateKeys()
    val parameters =
      Parameters(
        serializedParameters = privateMembershipSerializedParameters,
        serializedPrivateKey = keys.serializedPrivateKey,
        serializedPublicKey = keys.serializedPublicKey
      )
    val decryptedResults = runWorkflow(queryResultsDecryptor, parameters)
    assertThat(decryptedResults).satisfies {
      assertThat(
          it
            .map { dataset ->
              dataset.decryptedEventDataList.map { plaintext ->
                Pair(dataset.queryId.id, plaintext)
              }
            }
            .flatten()
        )
        .containsExactly(
          1 to plaintextOf("<some long data a>"),
          1 to plaintextOf("<some long data b>"),
          2 to plaintextOf("<some long data c>"),
          2 to plaintextOf("<some long data d>"),
          3 to plaintextOf("<some long data e>"),
        )
      null
    }
  }

  private fun makeCompressedEvents(
    plaintextCollection: PCollection<DecryptedEventDataSet>,
  ): CompressedEvents {
    val eventPCollection: PCollection<KV<ByteString, ByteString>> =
      plaintextCollection.parDo { dataSet: DecryptedEventDataSet ->
        dataSet.decryptedEventDataList.forEach {
          yield(kvOf(requireNotNull(dataSet.queryId).toByteString(), requireNotNull(it.payload)))
        }
      }
    return eventCompressorTrainer.compressByKey(eventPCollection)
  }

  private fun makeEncryptedResults(
    privateMembershipCryptorHelper: PrivateMembershipCryptorHelper,
    keys: PrivateMembershipKeys,
    joinkeyCollection: PCollection<KV<QueryId, JoinKey>>,
    events: PCollection<KV<ByteString, ByteString>>
  ): PCollection<EncryptedQueryResult> {

    val compressedPlaintexts: PCollection<KV<QueryId, DecryptedEventDataSet>> =
      events.map { kvOf(QueryId.parseFrom(it.key), it.value) }.groupByKey("Group By QueryId").map<
        KV<QueryId, Iterable<ByteString>>, KV<QueryId, DecryptedEventDataSet>> {
        kvOf(
          it.key,
          decryptedEventDataSet {
            decryptedEventData += it.value.map { plaintext { payload = it } }
            queryId = it.key
          }
        )
      }
    val encryptedEventDataSet: PCollection<EncryptedEventDataSet> =
      compressedPlaintexts.strictOneToOneJoin<QueryId, DecryptedEventDataSet, JoinKey>(
          joinkeyCollection
        )
        .map {
          privateMembershipCryptorHelper.makeEncryptedEventDataSet(
            it.key,
            it.key.queryId to it.value
          )
        }
    return encryptedEventDataSet.map {
      privateMembershipCryptorHelper.makeEncryptedQueryResult(keys, it)
    }
  }
}
