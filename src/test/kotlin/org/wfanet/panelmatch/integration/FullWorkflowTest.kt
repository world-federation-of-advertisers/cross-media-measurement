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

package org.wfanet.panelmatch.integration

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import kotlin.test.assertNotNull
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.flatten
import org.wfanet.panelmatch.client.PreprocessEventsRequestKt.unprocessedEvent
import org.wfanet.panelmatch.client.common.databaseEntryOf
import org.wfanet.panelmatch.client.common.databaseKeyOf
import org.wfanet.panelmatch.client.common.joinKeyAndIdOf
import org.wfanet.panelmatch.client.common.plaintextOf
import org.wfanet.panelmatch.client.eventpreprocessing.JniEventPreprocessor
import org.wfanet.panelmatch.client.exchangetasks.joinKeyAndIdCollection
import org.wfanet.panelmatch.client.preprocessEventsRequest
import org.wfanet.panelmatch.client.privatemembership.DatabaseEntry
import org.wfanet.panelmatch.client.privatemembership.keyedDecryptedEventDataSet
import org.wfanet.panelmatch.common.compression.CompressionParametersKt.brotliCompressionParameters
import org.wfanet.panelmatch.common.compression.compressionParameters
import org.wfanet.panelmatch.common.parseDelimitedMessages
import org.wfanet.panelmatch.common.toDelimitedByteString

private val PLAINTEXT_JOIN_KEYS = joinKeyAndIdCollection {
  joinKeyAndIds +=
    joinKeyAndIdOf("join-key-1".toByteStringUtf8(), "join-key-id-1".toByteStringUtf8())
  joinKeyAndIds +=
    joinKeyAndIdOf("join-key-2".toByteStringUtf8(), "join-key-id-2".toByteStringUtf8())
}

private val EDP_COMMUTATIVE_DETERMINISTIC_KEY = "some-key".toByteStringUtf8()
private val EDP_IDENTIFIER_HASH_PEPPER = "edp-identifier-hash-pepper".toByteStringUtf8()
private val EDP_HKDF_PEPPER = "edp-hkdf-pepper".toByteStringUtf8()
private val EDP_COMPRESSION_PARAMETERS = compressionParameters {
  brotli = brotliCompressionParameters { dictionary = ByteString.EMPTY }
}
private val EDP_ENCRYPTED_EVENT_DATA_MANIFEST = "edp-encrypted-event-data-?-of-1".toByteStringUtf8()

private fun makeDatabaseEntry(index: Int): DatabaseEntry {
  val request = preprocessEventsRequest {
    cryptoKey = EDP_COMMUTATIVE_DETERMINISTIC_KEY
    hkdfPepper = EDP_HKDF_PEPPER
    identifierHashPepper = EDP_IDENTIFIER_HASH_PEPPER
    compressionParameters = EDP_COMPRESSION_PARAMETERS
    unprocessedEvents +=
      unprocessedEvent {
        id = "join-key-$index".toByteStringUtf8()
        data = "payload-for-join-key-$index".toByteStringUtf8()
      }
  }
  val response = JniEventPreprocessor().preprocess(request)
  val processedEvent = response.processedEventsList.single()
  return databaseEntryOf(
    databaseKeyOf(processedEvent.encryptedId),
    plaintextOf(processedEvent.encryptedData)
  )
}

private val EDP_DATABASE_ENTRIES = (0 until 100).map { makeDatabaseEntry(it) }

private val EDP_ENCRYPTED_EVENT_DATA_BLOB =
  EDP_DATABASE_ENTRIES.map { it.toDelimitedByteString() }.flatten()

@RunWith(JUnit4::class)
class FullWorkflowTest : AbstractInProcessPanelMatchIntegrationTest() {
  override val exchangeWorkflowResourcePath: String = "config/full_exchange_workflow.textproto"

  override val initialDataProviderInputs: Map<String, ByteString> =
    mapOf(
      "edp-identifier-hash-pepper" to EDP_IDENTIFIER_HASH_PEPPER,
      "edp-commutative-deterministic-key" to EDP_COMMUTATIVE_DETERMINISTIC_KEY,
      "edp-encrypted-event-data" to EDP_ENCRYPTED_EVENT_DATA_MANIFEST,
      "edp-encrypted-event-data-0-of-1" to EDP_ENCRYPTED_EVENT_DATA_BLOB,
      "edp-compression-parameters" to EDP_COMPRESSION_PARAMETERS.toByteString(),
      "edp-hkdf-pepper" to EDP_HKDF_PEPPER,
      "edp-previous-single-blinded-join-keys" to ByteString.EMPTY,
    )

  override val initialModelProviderInputs: Map<String, ByteString> =
    mapOf(
      "mp-plaintext-join-keys" to PLAINTEXT_JOIN_KEYS.toByteString(),
    )

  override fun validateFinalState(
    dataProviderDaemon: ExchangeWorkflowDaemonForTest,
    modelProviderDaemon: ExchangeWorkflowDaemonForTest
  ) {
    val blob = modelProviderDaemon.readPrivateBlob("decrypted-event-data-0-of-1")
    assertNotNull(blob)

    val decryptedEvents =
      blob.parseDelimitedMessages(keyedDecryptedEventDataSet {}).map {
        val payload =
          it.decryptedEventDataList.joinToString("") { plaintext ->
            plaintext.payload.toStringUtf8()
          }
        it.plaintextJoinKeyAndId.joinKey.key.toStringUtf8() to payload
      }
    assertThat(decryptedEvents)
      .containsExactly(
        "join-key-1" to "payload-for-join-key-1",
        "join-key-2" to "payload-for-join-key-2",
      )
  }
}
