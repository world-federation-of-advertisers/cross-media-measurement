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

package org.wfanet.panelmatch.client.eventpreprocessing.testing

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.client.CombinedEvents
import org.wfanet.panelmatch.client.common.CompressedEvents
import org.wfanet.panelmatch.client.common.testing.FakeDictionaryBuilder
import org.wfanet.panelmatch.client.common.testing.eventsOf
import org.wfanet.panelmatch.client.eventpreprocessing.compressByKey
import org.wfanet.panelmatch.common.beam.testing.BeamTestBase
import org.wfanet.panelmatch.common.beam.testing.assertThat
import org.wfanet.panelmatch.common.compression.testing.FakeCompressor

@RunWith(JUnit4::class)
class FakeCompressorCompressByKeyTest : BeamTestBase() {

  @Test
  fun compressByKey() {
    val events = eventsOf("A" to "W", "A" to "X", "B" to "Y", "C" to "Z")

    val compressedEvents: CompressedEvents = FakeDictionaryBuilder().compressByKey(events)

    assertThat(compressedEvents.events).satisfies {
      val decodedEvents =
        it.map { kv ->
          assertThat(kv.value.startsWith(FakeCompressor.PREFIX.toByteStringUtf8()))
          val suffix = kv.value.substring(FakeCompressor.PREFIX.toByteStringUtf8().size())
          val combinedEvents = CombinedEvents.parseFrom(suffix)
          val stringEvents =
            combinedEvents.serializedEventsList.map(ByteString::toStringUtf8).sorted()
          kv.key.toStringUtf8() to stringEvents
        }
      assertThat(decodedEvents)
        .containsExactly(
          "A" to listOf("W", "X"),
          "B" to listOf("Y"),
          "C" to listOf("Z"),
        )
      null
    }

    assertThat(compressedEvents.dictionary).satisfies {
      val dictionary = it.toList()
      assertThat(dictionary.single().contents.toStringUtf8())
        .isAnyOf(
          "Dictionary: W, X, Y",
          "Dictionary: W, X, Z",
          "Dictionary: W, Y, Z",
          "Dictionary: X, Y, Z"
        )
      null
    }
  }
}
