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
import org.apache.beam.sdk.values.PCollection
import org.wfanet.panelmatch.client.CombinedEvents
import org.wfanet.panelmatch.client.privatemembership.DecryptedEventData
import org.wfanet.panelmatch.client.privatemembership.decryptedEventData
import org.wfanet.panelmatch.common.beam.parDoWithSideInput
import org.wfanet.panelmatch.common.compression.Compressor
import org.wfanet.panelmatch.common.compression.CompressorFactory
import org.wfanet.panelmatch.common.compression.FactoryBasedCompressor

/**
 * Receives [PCollection] of events and a dictionary. Compresses the events using the [Compressor]
 * generated using the [CompressorFactory] and dictionary.
 */
fun uncompressEvents(
  compressedEvents: PCollection<DecryptedEventData>,
  dictionary: PCollection<ByteString>,
  compressorFactory: CompressorFactory
): PCollection<DecryptedEventData> {

  return compressedEvents.parDoWithSideInput(dictionary.apply(View.asSingleton())) {
    events: DecryptedEventData,
    dictionaryData: ByteString ->
    val compressor: Compressor = FactoryBasedCompressor(dictionaryData, compressorFactory)
    CombinedEvents.parseFrom(compressor.uncompress(events.plaintext)).serializedEventsList.forEach {
      yield(
        decryptedEventData {
          plaintext = it
          queryId = events.queryId
          shardId = events.shardId
        }
      )
    }
  }
}
