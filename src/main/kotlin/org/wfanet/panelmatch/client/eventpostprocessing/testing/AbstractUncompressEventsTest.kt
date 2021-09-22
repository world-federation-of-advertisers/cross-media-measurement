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
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.client.common.CompressedEvents
import org.wfanet.panelmatch.client.common.EventCompressorTrainer
import org.wfanet.panelmatch.client.common.testing.eventsOf
import org.wfanet.panelmatch.client.eventpostprocessing.uncompressEvents
import org.wfanet.panelmatch.client.eventpreprocessing.compressByKey
import org.wfanet.panelmatch.client.privatemembership.DecryptedEventData
import org.wfanet.panelmatch.client.privatemembership.decryptedEventData
import org.wfanet.panelmatch.client.privatemembership.queryId
import org.wfanet.panelmatch.client.privatemembership.shardId
import org.wfanet.panelmatch.common.beam.map
import org.wfanet.panelmatch.common.beam.testing.BeamTestBase
import org.wfanet.panelmatch.common.beam.testing.assertThat
import org.wfanet.panelmatch.common.compression.CompressorFactory
import org.wfanet.panelmatch.common.toByteString

@RunWith(JUnit4::class)
abstract class AbstractUncompressEventsTest : BeamTestBase() {
  abstract val eventCompressorTrainer: EventCompressorTrainer
  abstract val compressorFactory: CompressorFactory

  @Test
  fun uncompressEvents() {
    val events = eventsOf("A" to "W", "A" to "X", "B" to "Y", "C" to "Z")
    val compressedEvents: CompressedEvents = eventCompressorTrainer.compressByKey(events)
    val eventData =
      compressedEvents.events.map {
        decryptedEventData {
          plaintext = it.value
          this.queryId = queryId { id = it.key.toStringUtf8().first().toInt() }
        }
      }
    val uncompressedEvents =
      uncompressEvents(eventData, compressedEvents.dictionary, compressorFactory)
    assertThat(uncompressedEvents)
      .containsInAnyOrder(
        plaintextOf("W", 65),
        plaintextOf("X", 65),
        plaintextOf("Y", 66),
        plaintextOf("Z", 67),
      )
  }
  private fun plaintextOf(plaintext: String, query: Int): DecryptedEventData {
    return decryptedEventData {
      this.plaintext = plaintext.toByteString()
      this.queryId = queryId { id = query }
      this.shardId = shardId {}
    }
  }
}
