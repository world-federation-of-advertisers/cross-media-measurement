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
import com.google.protobuf.ByteString
import org.apache.beam.sdk.values.KV
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.client.common.UncompressedDictionaryBuilder
import org.wfanet.panelmatch.client.common.testing.eventsOf
import org.wfanet.panelmatch.common.beam.testing.BeamTestBase
import org.wfanet.panelmatch.common.beam.testing.assertThat
import org.wfanet.panelmatch.common.compression.dictionary
import org.wfanet.panelmatch.common.toByteString

private const val MAX_BYTE_SIZE = 8
private val IDENTIFIER_HASH_PEPPER_PROVIDER =
  HardCodedIdentifierHashPepperProvider("identifier-hash-pepper".toByteString())
private val HKDF_PEPPER_PROVIDER = HardCodedHkdfPepperProvider("hkdf-pepper".toByteString())
private val CRYPTO_KEY_PROVIDER =
  HardCodedDeterministicCommutativeCipherKeyProvider("crypto-key".toByteString())

/** Unit tests for [preprocessEventsInPipeline]. */
@RunWith(JUnit4::class)
class PreprocessEventsInPipelineTest : BeamTestBase() {

  @Test
  fun hardCodedProviders() {
    val events = eventsOf("A" to "B", "C" to "D")

    val (encryptedEvents, dictionary) =
      preprocessEventsInPipeline(
        events,
        MAX_BYTE_SIZE,
        IDENTIFIER_HASH_PEPPER_PROVIDER,
        HKDF_PEPPER_PROVIDER,
        CRYPTO_KEY_PROVIDER,
        UncompressedDictionaryBuilder()
      )

    assertThat(encryptedEvents).satisfies {
      val results: List<KV<Long, ByteString>> = it.toList()
      assertThat(results).hasSize(2)
      assertThat(results.map { kv -> kv.value })
        .containsNoneOf("B".toByteString(), "D".toByteString())
      null
    }

    assertThat(dictionary).satisfies {
      assertThat(it).containsExactly(dictionary {})
      null
    }
  }
}
