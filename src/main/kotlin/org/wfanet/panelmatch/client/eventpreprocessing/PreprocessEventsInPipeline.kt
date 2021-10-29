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
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionTuple
import org.apache.beam.sdk.values.TupleTag
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
 * PTransform to encrypt and hash a set of compressed events. Processes batches to minimize
 * serialization expense.
 */
class EncryptEventsInBatches(
  private val maxByteSize: Int,
  private val identifierHashPepperProvider: IdentifierHashPepperProvider,
  private val hkdfPepperProvider: HkdfPepperProvider,
  private val cryptoKeyProvider: DeterministicCommutativeCipherKeyProvider,
) : PTransform<PCollection<KV<ByteString, ByteString>>, PCollection<KV<Long, ByteString>>>() {

  override fun expand(
    compressedEvents: PCollection<KV<ByteString, ByteString>>
  ): PCollection<KV<Long, ByteString>> {
    return compressedEvents
      .parDo(BatchingDoFn(maxByteSize, EventSize), name = "Batch by $maxByteSize bytes")
      .parDo(
        EncryptEventsDoFn(
          EncryptEvents(),
          identifierHashPepperProvider,
          hkdfPepperProvider,
          cryptoKeyProvider
        ),
        name = "Encrypt in Batches"
      )
  }
}

/**
 * PTransform to prepares event data for usage in Private Membership query evaluation.
 *
 * The basic steps are:
 *
 * 1. Compress values per key using [dictionaryBuilder].
 * 2. Batch these into collections of at most [maxByteSize] bytes.
 * 3. Encrypt the keys and values.
 */
class PreprocessEventsInPipeline(
  private val maxByteSize: Int,
  private val identifierHashPepperProvider: IdentifierHashPepperProvider,
  private val hkdfPepperProvider: HkdfPepperProvider,
  private val cryptoKeyProvider: DeterministicCommutativeCipherKeyProvider,
  private val dictionaryBuilder: DictionaryBuilder
) : PTransform<PCollectionTuple, PCollectionTuple>() {

  override fun expand(input: PCollectionTuple): PCollectionTuple {
    val unprocessedEvents = input[unprocessedEventsTag]
    val compressedEvents: CompressedEvents = dictionaryBuilder.compressByKey(unprocessedEvents)
    val preprocessedEvents: PCollection<KV<Long, ByteString>> =
      compressedEvents.events.apply(
        "Encrypt Events",
        EncryptEventsInBatches(
          maxByteSize,
          identifierHashPepperProvider,
          hkdfPepperProvider,
          cryptoKeyProvider
        )
      )
    return PCollectionTuple.of(preprocessedEventsTag, preprocessedEvents)
      .and(dictionaryTag, compressedEvents.dictionary)
  }

  companion object {
    val unprocessedEventsTag = TupleTag<KV<ByteString, ByteString>>()
    val preprocessedEventsTag = TupleTag<KV<Long, ByteString>>()
    val dictionaryTag = TupleTag<Dictionary>()
  }
}

/** Convenience function for performing a PTransform of processing unprocessed data. */
fun preprocessEventsInPipeline(
  events: PCollection<KV<ByteString, ByteString>>,
  maxByteSize: Int,
  identifierHashPepperProvider: IdentifierHashPepperProvider,
  hkdfPepperProvider: HkdfPepperProvider,
  cryptoKeyProvider: DeterministicCommutativeCipherKeyProvider,
  dictionaryBuilder: DictionaryBuilder
): PreprocessedEvents {
  val tuple =
    PCollectionTuple.of(PreprocessEventsInPipeline.unprocessedEventsTag, events)
      .apply(
        "Process Events",
        PreprocessEventsInPipeline(
          maxByteSize,
          identifierHashPepperProvider,
          hkdfPepperProvider,
          cryptoKeyProvider,
          dictionaryBuilder
        )
      )
  return PreprocessedEvents(
    events = tuple[PreprocessEventsInPipeline.preprocessedEventsTag],
    dictionary = tuple[PreprocessEventsInPipeline.dictionaryTag]
  )
}
