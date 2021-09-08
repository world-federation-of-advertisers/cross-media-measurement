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
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.client.eventpreprocessing.EventAggregatorTrainer.TrainedEventAggregator
import org.wfanet.panelmatch.client.eventpreprocessing.testing.eventsOf
import org.wfanet.panelmatch.common.beam.kvOf
import org.wfanet.panelmatch.common.beam.map
import org.wfanet.panelmatch.common.beam.testing.BeamTestBase
import org.wfanet.panelmatch.common.beam.testing.assertThat
import org.wfanet.panelmatch.common.toByteString

@RunWith(JUnit4::class)
class EventAggregatorsTest : BeamTestBase() {

  @Test
  fun aggregateByKey() {
    val events = eventsOf("A" to "W", "A" to "X", "B" to "Y", "C" to "Z")

    val aggregatedEvents: AggregatedEvents = FakeEventAggregatorTrainer().aggregateByKey(events)

    val eventsAsStrings =
      aggregatedEvents.events.map { kvOf(it.key.toStringUtf8(), it.value.toStringUtf8()) }
    assertThat(eventsAsStrings)
      .containsInAnyOrder(
        kvOf("A", "Combined: W, X"),
        kvOf("B", "Combined: Y"),
        kvOf("C", "Combined: Z"),
      )

    assertThat(aggregatedEvents.dictionary).satisfies {
      val dictionary = it.toList()
      assertThat(dictionary).hasSize(1)
      assertThat(dictionary[0].toStringUtf8())
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

private class FakeEventAggregator : EventAggregator {
  override fun combine(events: Iterable<ByteString>): ByteString {
    return "Combined: ${events.sortAndJoin()}".toByteString()
  }
}

private class FakeEventAggregatorTrainer : EventAggregatorTrainer {
  override val preferredSampleSize: Int = 3

  override fun train(eventsSample: Iterable<ByteString>): TrainedEventAggregator {
    return TrainedEventAggregator(
      FakeEventAggregator(),
      "Dictionary: ${eventsSample.sortAndJoin()}".toByteString()
    )
  }
}

private fun Iterable<ByteString>.sortAndJoin(): String {
  return map { it.toStringUtf8() }.sorted().joinToString(", ")
}
