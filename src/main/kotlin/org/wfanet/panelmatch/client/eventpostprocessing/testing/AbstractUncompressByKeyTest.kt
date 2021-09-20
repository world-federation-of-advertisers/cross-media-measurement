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

package org.wfanet.panelmatch.client.eventpostprocessing.testing

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.client.common.CompressedEvents
import org.wfanet.panelmatch.client.common.EventCompressorTrainer
import org.wfanet.panelmatch.client.common.testing.eventsOf
import org.wfanet.panelmatch.client.eventpostprocessing.uncompressByKey
import org.wfanet.panelmatch.client.eventpreprocessing.compressByKey
import org.wfanet.panelmatch.common.beam.testing.BeamTestBase
import org.wfanet.panelmatch.common.beam.testing.assertThat
import org.wfanet.panelmatch.common.compression.Compressor

@RunWith(JUnit4::class)
abstract class AbstractUncompressByKeyTest : BeamTestBase() {
  abstract val eventCompressorTrainer: EventCompressorTrainer
  abstract val getCompressor: (ByteString) -> Compressor

  @Test
  fun compressByKey() {
    val events = eventsOf("A" to "W", "A" to "X", "B" to "Y", "C" to "Z")
    val compressedEvents: CompressedEvents = eventCompressorTrainer.compressByKey(events)
    val uncompressedEvents = uncompressByKey(compressedEvents, getCompressor)
    assertThat(uncompressedEvents).satisfies {
      assertThat(
          it.map {
            requireNotNull(it.key.toStringUtf8()) to requireNotNull(it.value.toStringUtf8())
          }
        )
        .containsExactlyElementsIn(listOf("A" to "W", "A" to "X", "B" to "Y", "C" to "Z"))
      null
    }
  }
}
