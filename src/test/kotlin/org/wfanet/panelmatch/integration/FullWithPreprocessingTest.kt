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
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.common.flatten
import org.wfanet.panelmatch.client.common.joinKeyOf
import org.wfanet.panelmatch.client.common.unprocessedEventOf
import org.wfanet.panelmatch.client.eventpreprocessing.UnprocessedEvent
import org.wfanet.panelmatch.client.exchangetasks.joinKeyCollection
import org.wfanet.panelmatch.client.privatemembership.keyedDecryptedEventDataSet
import org.wfanet.panelmatch.common.compression.CompressionParametersKt.brotliCompressionParameters
import org.wfanet.panelmatch.common.compression.compressionParameters
import org.wfanet.panelmatch.common.parseDelimitedMessages
import org.wfanet.panelmatch.common.toDelimitedByteString
import org.wfanet.panelmatch.integration.testing.parsePlaintextResults

private val PLAINTEXT_JOIN_KEYS = joinKeyCollection {
  joinKeys += joinKeyOf("join-key-1".toByteStringUtf8())
  joinKeys += joinKeyOf("join-key-2".toByteStringUtf8())
}

private val EDP_COMPRESSION_PARAMETERS = compressionParameters {
  brotli = brotliCompressionParameters { dictionary = ByteString.EMPTY }
}
private val EDP_EVENT_DATA_MANIFEST = "edp-event-data-?-of-1".toByteStringUtf8()

private val EDP_DATABASE_ENTRIES: List<UnprocessedEvent> =
  (0 until 100).flatMap { index ->
    listOf(
      unprocessedEventOf(
        "join-key-$index".toByteStringUtf8(),
        "payload-1-for-join-key-$index".toByteStringUtf8(),
      ),
      unprocessedEventOf(
        "join-key-$index".toByteStringUtf8(),
        "payload-2-for-join-key-$index".toByteStringUtf8(),
      ),
    )
  }

private val EDP_EVENT_DATA_BLOB = EDP_DATABASE_ENTRIES.map { it.toDelimitedByteString() }.flatten()

@RunWith(JUnit4::class)
class FullWithPreprocessingTest : AbstractInProcessPanelMatchIntegrationTest() {
  override val exchangeWorkflowResourcePath: String = "config/full_with_preprocessing.textproto"

  override val workflow: ExchangeWorkflow by lazy {
    readExchangeWorkflowTextProto(exchangeWorkflowResourcePath)
  }

  override val initialDataProviderInputs: Map<String, ByteString> =
    mapOf(
      "edp-event-data" to EDP_EVENT_DATA_MANIFEST,
      "edp-event-data-0-of-1" to EDP_EVENT_DATA_BLOB,
      "edp-compression-parameters" to EDP_COMPRESSION_PARAMETERS.toByteString(),
      "edp-previous-single-blinded-join-keys" to ByteString.EMPTY,
    )

  override val initialModelProviderInputs: Map<String, ByteString> =
    mapOf("mp-plaintext-join-keys" to PLAINTEXT_JOIN_KEYS.toByteString())

  override fun validateFinalState(
    dataProviderDaemon: ExchangeWorkflowDaemonForTest,
    modelProviderDaemon: ExchangeWorkflowDaemonForTest,
  ) {
    val blob = modelProviderDaemon.readPrivateBlob("decrypted-event-data-0-of-1")
    assertNotNull(blob)

    val decryptedEvents =
      parsePlaintextResults(blob.parseDelimitedMessages(keyedDecryptedEventDataSet {})).map {
        it.joinKey to it.plaintexts
      }
    assertThat(decryptedEvents)
      .containsExactly(
        "join-key-1" to listOf("payload-1-for-join-key-1", "payload-2-for-join-key-1"),
        "join-key-2" to listOf("payload-1-for-join-key-2", "payload-2-for-join-key-2"),
      )
  }
}
