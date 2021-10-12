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
import org.apache.beam.sdk.transforms.Sample
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.wfanet.panelmatch.client.combinedEvents
import org.wfanet.panelmatch.client.common.CompressedEvents
import org.wfanet.panelmatch.client.common.DictionaryBuilder
import org.wfanet.panelmatch.client.common.buildAsPCollectionView
import org.wfanet.panelmatch.common.beam.groupByKey
import org.wfanet.panelmatch.common.beam.kvOf
import org.wfanet.panelmatch.common.beam.map
import org.wfanet.panelmatch.common.beam.mapValues
import org.wfanet.panelmatch.common.beam.mapWithSideInput
import org.wfanet.panelmatch.common.beam.values
import org.wfanet.panelmatch.common.compression.Compressor
import org.wfanet.panelmatch.common.compression.Dictionary

/**
 * First use [Sample.any] -- which is not guaranteed to be uniform -- to significantly over-sample
 * then use [Sample.fixedSizeGlobally], which is uniform random but inefficient on lots of data.
 *
 * We coarsely sample 10 times as much data as is necessary; this should be adjusted experimentally.
 */
private const val OVERSAMPLING_FACTOR = 10L

/** Trains a [Compressor] and applies it to a [PCollection]. */
fun DictionaryBuilder.compressByKey(
  events: PCollection<KV<ByteString, ByteString>>
): CompressedEvents {

  val dictionary: PCollection<Dictionary> =
    events
      .values()
      .apply("Rough Sample", Sample.any(OVERSAMPLING_FACTOR * preferredSampleSize))
      .apply("Uniform Sample", Sample.fixedSizeGlobally(preferredSampleSize))
      .map("Train Compressor") { buildDictionary(it) }

  val compressorView = factory.buildAsPCollectionView(dictionary)

  val compressedEvents: PCollection<KV<ByteString, ByteString>> =
    events
      .groupByKey()
      .mapValues { combinedEvents { serializedEvents += it }.toByteString() }
      .mapWithSideInput(compressorView, name = "Compress") {
        keyAndEvents: KV<ByteString, ByteString>,
        compressor: Compressor ->
        kvOf(keyAndEvents.key, compressor.compress(keyAndEvents.value))
      }

  return CompressedEvents(compressedEvents, dictionary)
}
