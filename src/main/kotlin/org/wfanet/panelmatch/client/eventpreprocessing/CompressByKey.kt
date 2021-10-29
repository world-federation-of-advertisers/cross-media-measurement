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
import org.apache.beam.sdk.values.PCollectionTuple
import org.wfanet.panelmatch.client.common.CompressedEvents
import org.wfanet.panelmatch.common.beam.values
import org.wfanet.panelmatch.common.compression.Compressor
import org.wfanet.panelmatch.common.compression.Dictionary
import org.wfanet.panelmatch.common.compression.DictionaryBuilder

/** Trains a [Compressor] and applies it to a [PCollection]. */
fun DictionaryBuilder.compressByKey(
  events: PCollection<KV<ByteString, ByteString>>
): CompressedEvents {

  val dictionary: PCollection<Dictionary> =
    events.values().apply("Create Dictionary", BuildDictionary(this))

  val compressedEvents =
    PCollectionTuple.of(CompressEvents.eventsTag, events)
      .and(CompressEvents.dictionaryTag, dictionary)
      .apply("Compress Events", CompressEvents(this))

  return CompressedEvents(compressedEvents, dictionary)
}
