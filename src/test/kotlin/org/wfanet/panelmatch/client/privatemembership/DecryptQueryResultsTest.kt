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

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Any
import com.google.protobuf.ByteString
import com.google.protobuf.StringValue
import com.google.protobuf.kotlin.toByteStringUtf8
import com.google.protobuf.stringValue
import org.apache.beam.sdk.values.PCollection
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.client.common.joinKeyIdentifierOf
import org.wfanet.panelmatch.client.common.queryIdOf
import org.wfanet.panelmatch.client.exchangetasks.JoinKeyAndId
import org.wfanet.panelmatch.client.exchangetasks.JoinKeyIdentifier
import org.wfanet.panelmatch.client.privatemembership.testing.joinKeyAndIdOf
import org.wfanet.panelmatch.client.privatemembership.testing.queryIdAndIdOf
import org.wfanet.panelmatch.common.beam.testing.BeamTestBase
import org.wfanet.panelmatch.common.beam.testing.assertThat
import org.wfanet.panelmatch.common.compression.CompressionParametersKt.brotliCompressionParameters
import org.wfanet.panelmatch.common.compression.compressionParameters
import org.wfanet.panelmatch.common.crypto.AsymmetricKeyPair

private val QUERY_ID_AND_IDS: List<QueryIdAndId> =
  listOf(
    queryIdAndIdOf(1, "some-id-1"),
    queryIdAndIdOf(2, "some-id-2"),
    queryIdAndIdOf(3, "some-id-3"),
  )

private val QUERY_ID_AND_IDS_SOME_DISCARDED: List<QueryIdAndId> =
  listOf(queryIdAndIdOf(1, "some-id-1"), queryIdAndIdOf(3, "some-id-3"))

private val DISCARDED_JOIN_KEYS: List<JoinKeyIdentifier> =
  listOf(joinKeyIdentifierOf("some-id-2".toByteStringUtf8()))

private val PLAINTEXT_JOIN_KEY_AND_IDS: List<JoinKeyAndId> =
  listOf(
    joinKeyAndIdOf("some-plaintext-joinkey-1", "some-id-1"),
    joinKeyAndIdOf("some-plaintext-joinkey-2", "some-id-2"),
    joinKeyAndIdOf("some-plaintext-joinkey-3", "some-id-3"),
  )

private val DECRYPTED_JOIN_KEY_AND_IDS: List<JoinKeyAndId> =
  listOf(
    joinKeyAndIdOf("some-decrypted-join-key-id-1", "some-id-1"),
    joinKeyAndIdOf("some-decrypted-join-key-id-2", "some-id-2"),
    joinKeyAndIdOf("some-decrypted-join-key-id-3", "some-id-3"),
  )

private val HKDF_PEPPER = "some-pepper".toByteStringUtf8()

private val ASYMMETRIC_KEYS =
  AsymmetricKeyPair("public-key".toByteStringUtf8(), "private-key".toByteStringUtf8())

private val COMPRESSION_PARAMETERS = compressionParameters {
  brotli = brotliCompressionParameters { dictionary = "some-dictionary".toByteStringUtf8() }
}
private const val PRIVATE_MEMBERSHIP_PARAMETERS = "some serialized parameters"

@RunWith(JUnit4::class)
class DecryptQueryResultsTest : BeamTestBase() {
  @Test
  fun success() {
    val encryptedQueryResultsList =
      listOf(
        encryptedQueryResultOf(1, "payload-1"),
        encryptedQueryResultOf(2, "payload-2"),
        encryptedQueryResultOf(3, "payload-3"),
      )
    val encryptedQueryResults =
      pcollectionOf("Create EncryptedQueryResults", encryptedQueryResultsList)
    val queryIdAndIds: PCollection<QueryIdAndId> =
      pcollectionOf("Create QueryIdAndIds", QUERY_ID_AND_IDS)
    val plaintextJoinKeyAndIds: PCollection<JoinKeyAndId> =
      pcollectionOf("Create PlaintextJoinKeyAndIds", PLAINTEXT_JOIN_KEY_AND_IDS)
    val decryptedJoinKeyAndIds: PCollection<JoinKeyAndId> =
      pcollectionOf("Create DecryptedJoinKeyAndIds", DECRYPTED_JOIN_KEY_AND_IDS)

    val results =
      decryptQueryResults(
        encryptedQueryResults = encryptedQueryResults,
        plaintextJoinKeyAndIds = plaintextJoinKeyAndIds,
        decryptedJoinKeyAndIds = decryptedJoinKeyAndIds,
        queryIdAndIds = queryIdAndIds,
        compressionParameters =
          pcollectionViewOf("CompressionParameters View", COMPRESSION_PARAMETERS),
        privateMembershipKeys = pcollectionViewOf("Keys View", ASYMMETRIC_KEYS),
        parameters = Any.pack(stringValue { value = PRIVATE_MEMBERSHIP_PARAMETERS }),
        queryResultsDecryptor = TestQueryResultsDecryptor,
        hkdfPepper = HKDF_PEPPER,
      )

    assertThat(results).satisfies { keyedDecryptedEventDataSets ->
      val deserializedResults: List<Pair<String, List<ByteString>>> =
        keyedDecryptedEventDataSets.map {
          it.plaintextJoinKeyAndId.joinKey.key.toStringUtf8() to
            it.decryptedEventDataList.map { plaintext -> plaintext.payload }
        }

      assertThat(deserializedResults)
        .containsExactly(
          "some-plaintext-joinkey-1" to
            listOf(
              "some-decrypted-join-key-id-1".toByteStringUtf8(),
              HKDF_PEPPER,
              PRIVATE_MEMBERSHIP_PARAMETERS.toByteStringUtf8(),
              COMPRESSION_PARAMETERS.toByteString(),
              ASYMMETRIC_KEYS.serializedPublicKey,
              ASYMMETRIC_KEYS.serializedPrivateKey,
              "payload-1".toByteStringUtf8(),
            ),
          "some-plaintext-joinkey-2" to
            listOf(
              "some-decrypted-join-key-id-2".toByteStringUtf8(),
              HKDF_PEPPER,
              PRIVATE_MEMBERSHIP_PARAMETERS.toByteStringUtf8(),
              COMPRESSION_PARAMETERS.toByteString(),
              ASYMMETRIC_KEYS.serializedPublicKey,
              ASYMMETRIC_KEYS.serializedPrivateKey,
              "payload-2".toByteStringUtf8(),
            ),
          "some-plaintext-joinkey-3" to
            listOf(
              "some-decrypted-join-key-id-3".toByteStringUtf8(),
              HKDF_PEPPER,
              PRIVATE_MEMBERSHIP_PARAMETERS.toByteStringUtf8(),
              COMPRESSION_PARAMETERS.toByteString(),
              ASYMMETRIC_KEYS.serializedPublicKey,
              ASYMMETRIC_KEYS.serializedPrivateKey,
              "payload-3".toByteStringUtf8(),
            ),
        )

      null
    }
  }

  @Test
  fun successWithDiscardedJoinKeys() {
    val encryptedQueryResultsList =
      listOf(encryptedQueryResultOf(1, "payload-1"), encryptedQueryResultOf(3, "payload-3"))
    val encryptedQueryResults =
      pcollectionOf("Create EncryptedQueryResults", encryptedQueryResultsList)
    val queryIdAndIds: PCollection<QueryIdAndId> =
      pcollectionOf("Create QueryIdAndIds", QUERY_ID_AND_IDS_SOME_DISCARDED)
    val plaintextJoinKeyAndIds: PCollection<JoinKeyAndId> =
      pcollectionOf(
        "Create PlaintextJoinKeyAndIds",
        PLAINTEXT_JOIN_KEY_AND_IDS.removeDiscardedJoinKeys(DISCARDED_JOIN_KEYS),
      )
    val decryptedJoinKeyAndIds: PCollection<JoinKeyAndId> =
      pcollectionOf(
        "Create DecryptedJoinKeyAndIds",
        DECRYPTED_JOIN_KEY_AND_IDS.removeDiscardedJoinKeys(DISCARDED_JOIN_KEYS),
      )

    val results =
      decryptQueryResults(
        encryptedQueryResults = encryptedQueryResults,
        plaintextJoinKeyAndIds = plaintextJoinKeyAndIds,
        decryptedJoinKeyAndIds = decryptedJoinKeyAndIds,
        queryIdAndIds = queryIdAndIds,
        compressionParameters =
          pcollectionViewOf("CompressionParameters View", COMPRESSION_PARAMETERS),
        privateMembershipKeys = pcollectionViewOf("Keys View", ASYMMETRIC_KEYS),
        parameters = Any.pack(stringValue { value = PRIVATE_MEMBERSHIP_PARAMETERS }),
        queryResultsDecryptor = TestQueryResultsDecryptor,
        hkdfPepper = HKDF_PEPPER,
      )

    assertThat(results).satisfies { keyedDecryptedEventDataSets ->
      val deserializedResults: List<Pair<String, List<ByteString>>> =
        keyedDecryptedEventDataSets.map {
          it.plaintextJoinKeyAndId.joinKey.key.toStringUtf8() to
            it.decryptedEventDataList.map { plaintext -> plaintext.payload }
        }

      assertThat(deserializedResults)
        .containsExactly(
          "some-plaintext-joinkey-1" to
            listOf(
              "some-decrypted-join-key-id-1".toByteStringUtf8(),
              HKDF_PEPPER,
              PRIVATE_MEMBERSHIP_PARAMETERS.toByteStringUtf8(),
              COMPRESSION_PARAMETERS.toByteString(),
              ASYMMETRIC_KEYS.serializedPublicKey,
              ASYMMETRIC_KEYS.serializedPrivateKey,
              "payload-1".toByteStringUtf8(),
            ),
          "some-plaintext-joinkey-3" to
            listOf(
              "some-decrypted-join-key-id-3".toByteStringUtf8(),
              HKDF_PEPPER,
              PRIVATE_MEMBERSHIP_PARAMETERS.toByteStringUtf8(),
              COMPRESSION_PARAMETERS.toByteString(),
              ASYMMETRIC_KEYS.serializedPublicKey,
              ASYMMETRIC_KEYS.serializedPrivateKey,
              "payload-3".toByteStringUtf8(),
            ),
        )

      null
    }
  }
}

private object TestQueryResultsDecryptor : QueryResultsDecryptor {
  override fun decryptQueryResults(
    parameters: DecryptQueryResultsParameters
  ): DecryptQueryResultsResponse {
    return decryptQueryResultsResponse {
      // To ensure that things are properly flattened, we test two eventDataSets.

      // To ensure the request parameters are correct, we encode them in the first eventDataSet.
      eventDataSets += decryptedEventDataSet {
        decryptedEventData += plaintext { payload = parameters.decryptedJoinKey.key }
        decryptedEventData += plaintext { payload = parameters.hkdfPepper }
        decryptedEventData += plaintext {
          payload = parameters.parameters.unpack(StringValue::class.java).value.toByteStringUtf8()
        }
        decryptedEventData += plaintext {
          payload = parameters.compressionParameters.toByteString()
        }
        decryptedEventData += plaintext { payload = parameters.serializedPublicKey }
        decryptedEventData += plaintext { payload = parameters.serializedPrivateKey }
      }

      // To ensure the request encryptedQueryResults are correct, we encode them in an eventDataSet.
      eventDataSets += decryptedEventDataSet {
        for (result in parameters.encryptedQueryResults) {
          decryptedEventData += plaintext { payload = result.serializedEncryptedQueryResult }
        }
      }
    }
  }
}

private fun encryptedQueryResultOf(queryId: Int, payload: String): EncryptedQueryResult {
  return encryptedQueryResult {
    this.queryId = queryIdOf(queryId)
    serializedEncryptedQueryResult = payload.toByteStringUtf8()
  }
}
