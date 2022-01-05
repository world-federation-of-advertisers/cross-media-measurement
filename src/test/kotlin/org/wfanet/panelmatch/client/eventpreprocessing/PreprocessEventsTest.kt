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

package org.wfanet.panelmatch.client.eventpreprocessing

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.kotlin.toByteStringUtf8
import org.apache.beam.sdk.values.PCollection
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.client.common.unprocessedEventOf
import org.wfanet.panelmatch.common.beam.map
import org.wfanet.panelmatch.common.beam.testing.BeamTestBase
import org.wfanet.panelmatch.common.beam.testing.assertThat
import org.wfanet.panelmatch.common.compression.CompressionParametersKt.noCompression
import org.wfanet.panelmatch.common.compression.compressionParameters

private const val MAX_BYTE_SIZE = 8L
private val IDENTIFIER_HASH_PEPPER_PROVIDER =
  HardCodedIdentifierHashPepperProvider("identifier-hash-pepper".toByteStringUtf8())
private val HKDF_PEPPER_PROVIDER = HardCodedHkdfPepperProvider("hkdf-pepper".toByteStringUtf8())
private val CRYPTO_KEY_PROVIDER =
  HardCodedDeterministicCommutativeCipherKeyProvider("crypto-key".toByteStringUtf8())
private val COMPRESSION_PARAMETERS = compressionParameters { uncompressed = noCompression {} }

@RunWith(JUnit4::class)
class PreprocessEventsTest : BeamTestBase() {

  @Test
  fun hardCodedProviders() {
    val rawEvents: List<UnprocessedEvent> =
      listOf(
        unprocessedEventOf("id-1".toByteStringUtf8(), "event-1".toByteStringUtf8()),
        unprocessedEventOf("id-2".toByteStringUtf8(), "event-2".toByteStringUtf8()),
      )
    val unprocessedEvents: PCollection<UnprocessedEvent> =
      pcollectionOf("unprocessed events", rawEvents)
    val encryptedEvents =
      preprocessEvents(
        unprocessedEvents,
        MAX_BYTE_SIZE,
        IDENTIFIER_HASH_PEPPER_PROVIDER,
        HKDF_PEPPER_PROVIDER,
        CRYPTO_KEY_PROVIDER,
        JniEventPreprocessor(),
        pcollectionViewOf("Create Compression Parameters", COMPRESSION_PARAMETERS),
      )

    assertThat(encryptedEvents).satisfies {
      val results = it.toList()
      assertThat(results).hasSize(2)
      assertThat(results.map { databaseEntry -> databaseEntry.encryptedEntry.data })
        .containsNoneOf("event-1".toByteStringUtf8(), "event-2".toByteStringUtf8())
      null
    }
  }
}
