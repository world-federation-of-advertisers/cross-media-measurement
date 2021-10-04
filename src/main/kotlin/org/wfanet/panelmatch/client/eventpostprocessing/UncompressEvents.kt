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
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionView
import org.wfanet.panelmatch.client.CombinedEvents
import org.wfanet.panelmatch.client.privatemembership.DecryptedEventDataSet
import org.wfanet.panelmatch.client.privatemembership.Plaintext
import org.wfanet.panelmatch.client.privatemembership.decryptedEventDataSet
import org.wfanet.panelmatch.client.privatemembership.plaintext
import org.wfanet.panelmatch.common.beam.mapWithSideInput
import org.wfanet.panelmatch.common.compression.Compressor
import org.wfanet.panelmatch.common.compression.CompressorFactory
import org.wfanet.panelmatch.common.compression.FactoryBasedCompressor

/**
 * Receives [PCollection] of events and a dictionary. Compresses the events using the [Compressor]
 * generated using the [CompressorFactory] and dictionary.
 */
fun uncompressEvents(
  compressedEventSet: PCollection<DecryptedEventDataSet>,
  dictionary: PCollectionView<ByteString>,
  compressorFactory: CompressorFactory
): PCollection<DecryptedEventDataSet> {
  return compressedEventSet.mapWithSideInput(dictionary) {
    eventSet: DecryptedEventDataSet,
    dictionaryData: ByteString ->
    val compressor: Compressor = FactoryBasedCompressor(dictionaryData, compressorFactory)
    val uncompressedEvents: List<Plaintext> =
      eventSet.decryptedEventDataList.flatMap { events ->
        CombinedEvents.parseFrom(compressor.uncompress(events.payload)).serializedEventsList.map {
          plaintext { payload = it }
        }
      }
    decryptedEventDataSet {
      queryId = eventSet.queryId
      decryptedEventData += uncompressedEvents
    }
  }
}
