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

package org.wfanet.panelmatch.client.eventpostprocessing

import com.google.protobuf.ByteString
import org.apache.beam.sdk.transforms.View
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.wfanet.panelmatch.client.CombinedEvents
import org.wfanet.panelmatch.client.common.CompressedEvents
import org.wfanet.panelmatch.common.beam.kvOf
import org.wfanet.panelmatch.common.beam.map
import org.wfanet.panelmatch.common.beam.parDoWithSideInput
import org.wfanet.panelmatch.common.compression.Compressor

/**
 * Receives [CompressedEvents] consisting of a dictionary and events. It also receives a function to
 * generate a [Compressor] from a dictionary. Compresses the events using the [Compressor].
 */
fun uncompressByKey(
  compressedEvents: CompressedEvents,
  getCompressor: (ByteString) -> Compressor
): PCollection<KV<ByteString, ByteString>> {

  return compressedEvents.events.parDoWithSideInput(
    compressedEvents.dictionary.apply(View.asSingleton())
  ) { keyAndCompressedEvents: KV<ByteString, ByteString>, dictionary: ByteString ->
    val compressor = getCompressor(dictionary)
    CombinedEvents.parseFrom(compressor.uncompress(keyAndCompressedEvents.value))
      .serializedEventsList
      .map { yield(kvOf(keyAndCompressedEvents.key, it)) }
  }
}
