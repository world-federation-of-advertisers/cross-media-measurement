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
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.wfanet.panelmatch.client.common.CompressedEvents
import org.wfanet.panelmatch.common.beam.parDo
import org.wfanet.panelmatch.common.compression.Dictionary
import org.wfanet.panelmatch.common.compression.DictionaryBuilder

/**
 * Output of [preprocessEventsInPipeline].
 *
 * @property events preprocessed events (aggregated by key and compressed)
 * @property dictionary a dictionary necessary for decompression
 */
data class PreprocessedEvents(
  val events: PCollection<KV<Long, ByteString>>,
  val dictionary: PCollection<Dictionary>
)

/**
 * Prepares event data for usage in Private Membership query evaluation.
 *
 * The basic steps are:
 *
 * 1. Compress values per key using [dictionaryBuilder].
 * 2. Batch these into collections of at most [maxByteSize] bytes.
 * 3. Encrypt the keys and values.
 */
fun preprocessEventsInPipeline(
  events: PCollection<KV<ByteString, ByteString>>,
  maxByteSize: Int,
  identifierHashPepperProvider: IdentifierHashPepperProvider,
  hkdfPepperProvider: HkdfPepperProvider,
  cryptoKeyProvider: DeterministicCommutativeCipherKeyProvider,
  dictionaryBuilder: DictionaryBuilder
): PreprocessedEvents {
  val compressedEvents: CompressedEvents = dictionaryBuilder.compressByKey(events)

  val preprocessedEvents =
    compressedEvents
      .events
      .parDo(BatchingDoFn(maxByteSize, EventSize), name = "Batch by $maxByteSize bytes")
      .parDo(
        EncryptEventsDoFn(
          EncryptEvents(),
          identifierHashPepperProvider,
          hkdfPepperProvider,
          cryptoKeyProvider
        ),
        name = "Encrypt"
      )

  return PreprocessedEvents(preprocessedEvents, compressedEvents.dictionary)
}
