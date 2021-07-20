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

package org.wfanet.panelmatch.client.eventencryption

import com.google.protobuf.ByteString
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.coders.KvCoder
import org.apache.beam.sdk.coders.ListCoder
import org.apache.beam.sdk.extensions.protobuf.ByteStringCoder
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.client.PreprocessEventsRequest
import org.wfanet.panelmatch.client.PreprocessEventsResponse
import org.wfanet.panelmatch.client.eventencryption.EncryptionEventsDoFn as EncryptionEventsDoFn
import org.wfanet.panelmatch.common.beam.testing.BeamTestBase
import org.wfanet.panelmatch.common.beam.testing.assertThat

/** Unit tests for [EncryptionEventsDoFn]. */
@RunWith(JUnit4::class)
class EncryptionEventsDoFnTest : BeamTestBase() {
  private val coder: Coder<MutableList<KV<ByteString, ByteString>>> =
    ListCoder.of(KvCoder.of(ByteStringCoder.of(), ByteStringCoder.of()))

  @Test
  fun testEncrypt() {
    val arbitraryUnprocessedEvents: MutableList<KV<ByteString, ByteString>> =
      mutableListOf(byteStringKvOf("1", "2"), byteStringKvOf("3", "4"))
    val collection = pcollectionOf("collection1", arbitraryUnprocessedEvents, coder = coder)
    val doFn: DoFn<MutableList<KV<ByteString, ByteString>>, KV<ByteString, ByteString>> =
      EncryptionEventsDoFn(FakeEncryptEvents)
    val result: PCollection<KV<ByteString, ByteString>> = collection.apply(ParDo.of(doFn))
    assertThat(result)
      .containsInAnyOrder(
        byteStringKvOf("1abcdefg", "2hijklmnop"),
        byteStringKvOf("3abcdefg", "4hijklmnop")
      )
  }
  @Test
  fun testEmptyEncrypt() {
    val emptyUnprocessedEvents: MutableList<KV<ByteString, ByteString>> = mutableListOf()
    val emptycollection = pcollectionOf("collection1", emptyUnprocessedEvents, coder = coder)

    val doFn: DoFn<MutableList<KV<ByteString, ByteString>>, KV<ByteString, ByteString>> =
      EncryptionEventsDoFn(FakeEncryptEvents)
    val result: PCollection<KV<ByteString, ByteString>> = emptycollection.apply(ParDo.of(doFn))
    assertThat(result).empty()
  }
}

fun byteStringKvOf(key: String, value: String): KV<ByteString, ByteString> {
  return KV.of(ByteString.copyFromUtf8(key), ByteString.copyFromUtf8(value))
}

private object FakeEncryptEvents :
  SerializableFunction<PreprocessEventsRequest, PreprocessEventsResponse> {
  override fun apply(request: PreprocessEventsRequest): PreprocessEventsResponse {
    val response =
      PreprocessEventsResponse.newBuilder()
        .apply {
          for (events in request.unprocessedEventsList) {
            addProcessedEventsBuilder().apply {
              this.encryptedId = events.id.concat(ByteString.copyFromUtf8("abcdefg"))
              this.encryptedData = events.data.concat(ByteString.copyFromUtf8("hijklmnop"))
            }
          }
        }
        .build()
    return response
  }
}
