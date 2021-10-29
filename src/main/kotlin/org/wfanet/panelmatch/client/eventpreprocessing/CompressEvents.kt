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
import org.wfanet.panelmatch.client.combinedEvents
import org.wfanet.panelmatch.client.common.buildAsPCollectionView
import org.wfanet.panelmatch.common.beam.groupByKey
import org.wfanet.panelmatch.common.beam.kvOf
import org.wfanet.panelmatch.common.beam.mapValues
import org.wfanet.panelmatch.common.beam.mapWithSideInput
import org.wfanet.panelmatch.common.compression.Compressor
import org.wfanet.panelmatch.common.compression.Dictionary
import org.wfanet.panelmatch.common.compression.DictionaryBuilder

/**
 * PTransform for joining and compressing a collection of events. Takes a dictionary and constructs
 * a compressor from that dictionary. It then passes the compressor as side input to compress all
 * the joined events.
 */
class CompressEvents(private val dictionaryBuilder: DictionaryBuilder) :
  PTransform<PCollectionTuple, PCollection<KV<ByteString, ByteString>>>() {

  override fun expand(input: PCollectionTuple): PCollection<KV<ByteString, ByteString>> {
    val events = input[eventsTag]
    val dictionary = input[dictionaryTag]
    val compressorView = dictionaryBuilder.factory.buildAsPCollectionView(dictionary)
    return events
      .groupByKey()
      .mapValues { combinedEvents { serializedEvents += it }.toByteString() }
      .mapWithSideInput(compressorView, name = "Compress") {
        keyAndEvents: KV<ByteString, ByteString>,
        compressor: Compressor ->
        kvOf(keyAndEvents.key, compressor.compress(keyAndEvents.value))
      }
  }

  companion object {
    val eventsTag = TupleTag<KV<ByteString, ByteString>>()
    val dictionaryTag = TupleTag<Dictionary>()
  }
}
