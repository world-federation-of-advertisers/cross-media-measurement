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

import com.google.protobuf.ByteString
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.coders.KvCoder
import org.apache.beam.sdk.coders.ListCoder
import org.apache.beam.sdk.extensions.protobuf.ByteStringCoder
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.values.KV
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.client.PreprocessEventsRequest
import org.wfanet.panelmatch.client.PreprocessEventsResponse
import org.wfanet.panelmatch.client.PreprocessEventsResponseKt.processedEvent
import org.wfanet.panelmatch.client.preprocessEventsResponse
import org.wfanet.panelmatch.common.beam.kvOf
import org.wfanet.panelmatch.common.beam.parDo
import org.wfanet.panelmatch.common.beam.testing.BeamTestBase
import org.wfanet.panelmatch.common.beam.testing.assertThat
import org.wfanet.panelmatch.common.toByteString

private const val IDENTIFIER_HASH_PEPPER = "<some-identifier-hash-pepper>"
private const val HKDF_HASH_PEPPER = "<some-hkdf-hash-pepper>"
private const val CRYPTO_KEY = "<some-crypto-key>"
private const val EXPECTED_SUFFIX = "$IDENTIFIER_HASH_PEPPER$HKDF_HASH_PEPPER$CRYPTO_KEY"

/** Unit tests for [EncryptEventsDoFn]. */
@RunWith(JUnit4::class)
class EncryptEventsDoFnTest : BeamTestBase() {
  @Suppress("NULLABILITY_MISMATCH_BASED_ON_JAVA_ANNOTATIONS")
  private val coder: Coder<MutableList<KV<ByteString, ByteString>>> =
    ListCoder.of(KvCoder.of(ByteStringCoder.of(), ByteStringCoder.of()))

  @Test
  fun encrypt() {
    val arbitraryUnprocessedEvents: MutableList<KV<ByteString, ByteString>> =
      mutableListOf(inputOf("1000", "2"), inputOf("2000", "4"))
    val collection = pcollectionOf("collection1", arbitraryUnprocessedEvents, coder = coder)

    val doFn: DoFn<MutableList<KV<ByteString, ByteString>>, KV<Long, ByteString>> =
      EncryptEventsDoFn(
        FakeEncryptEvents,
        HardCodedIdentifierHashPepperProvider(IDENTIFIER_HASH_PEPPER.toByteString()),
        HardCodedHkdfPepperProvider(HKDF_HASH_PEPPER.toByteString()),
        HardCodedDeterministicCommutativeCipherKeyProvider(CRYPTO_KEY.toByteString())
      )

    assertThat(collection.parDo(doFn))
      .containsInAnyOrder(expectedOutputOf(1001, "2"), expectedOutputOf(2001, "4"))
  }
}

private fun inputOf(key: String, value: String): KV<ByteString, ByteString> {
  return kvOf(key.toByteString(), value.toByteString())
}

private fun outputOf(key: Long, value: String): KV<Long, ByteString> {
  return kvOf(key, value.toByteString())
}

private fun expectedOutputOf(key: Long, value: String): KV<Long, ByteString> {
  return outputOf(key, value + EXPECTED_SUFFIX)
}

private object FakeEncryptEvents :
  SerializableFunction<PreprocessEventsRequest, PreprocessEventsResponse> {
  override fun apply(request: PreprocessEventsRequest): PreprocessEventsResponse {
    val suffix = with(request) { identifierHashPepper.concat(hkdfPepper).concat(cryptoKey) }
    return preprocessEventsResponse {
      for (events in request.unprocessedEventsList) {
        processedEvents +=
          processedEvent {
            encryptedId = events.id.toStringUtf8().toLong() + 1
            encryptedData = events.data.concat(suffix)
          }
      }
    }
  }
}
