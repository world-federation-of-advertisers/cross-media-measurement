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
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.client.PreprocessEventsRequest
import org.wfanet.panelmatch.client.PreprocessEventsResponse
import org.wfanet.panelmatch.client.eventpreprocessing.EncryptionEventsDoFn as EncryptionEventsDoFn
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
      mutableListOf(inputOf("1000", "2"), inputOf("2000", "4"))
    val collection = pcollectionOf("collection1", arbitraryUnprocessedEvents, coder = coder)
    val doFn: DoFn<MutableList<KV<ByteString, ByteString>>, KV<Long, ByteString>> =
      EncryptionEventsDoFn(FakeEncryptEvents)
    val result: PCollection<KV<Long, ByteString>> = collection.apply(ParDo.of(doFn))
    assertThat(result)
      .containsInAnyOrder(outputOf(1001, "2hijklmnop"), outputOf(2001, "4hijklmnop"))
  }
  @Test
  fun testEmptyEncrypt() {
    val emptyUnprocessedEvents: MutableList<KV<ByteString, ByteString>> = mutableListOf()
    val emptycollection = pcollectionOf("collection1", emptyUnprocessedEvents, coder = coder)

    val doFn: DoFn<MutableList<KV<ByteString, ByteString>>, KV<Long, ByteString>> =
      EncryptionEventsDoFn(FakeEncryptEvents)
    val result: PCollection<KV<Long, ByteString>> = emptycollection.apply(ParDo.of(doFn))
    assertThat(result).empty()
  }
}

fun inputOf(key: String, value: String): KV<ByteString, ByteString> {
  return KV.of(ByteString.copyFromUtf8(key), ByteString.copyFromUtf8(value))
}

fun outputOf(key: Long, value: String): KV<Long, ByteString> {
  return KV.of(key, ByteString.copyFromUtf8(value))
}

private object FakeEncryptEvents :
  SerializableFunction<PreprocessEventsRequest, PreprocessEventsResponse> {
  override fun apply(request: PreprocessEventsRequest): PreprocessEventsResponse {
    val response =
      PreprocessEventsResponse.newBuilder()
        .apply {
          for (events in request.unprocessedEventsList) {
            addProcessedEventsBuilder().apply {
              this.encryptedId = events.id.toStringUtf8().toLong() + 1
              this.encryptedData = events.data.concat(ByteString.copyFromUtf8("hijklmnop"))
            }
          }
        }
        .build()
    return response
  }
}
