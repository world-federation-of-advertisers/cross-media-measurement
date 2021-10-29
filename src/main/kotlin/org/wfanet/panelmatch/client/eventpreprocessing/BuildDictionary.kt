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
import org.apache.beam.sdk.transforms.Sample
import org.apache.beam.sdk.values.PCollection
import org.wfanet.panelmatch.common.beam.map
import org.wfanet.panelmatch.common.compression.Dictionary
import org.wfanet.panelmatch.common.compression.DictionaryBuilder

/**
 * We coarsely sample 10 times as much data as is necessary; this should be adjusted experimentally.
 */
private const val OVERSAMPLING_FACTOR = 10L

/**
 * PTransform to build a dictionary from a set of events. First use [Sample.any] -- which is not
 * guaranteed to be uniform -- to significantly over-sample then use [Sample.fixedSizeGlobally],
 * which is uniform random but inefficient on lots of data.
 */
class BuildDictionary(private val dictionaryBuilder: DictionaryBuilder) :
  PTransform<PCollection<ByteString>, PCollection<Dictionary>>() {

  override fun expand(events: PCollection<ByteString>): PCollection<Dictionary> {
    return events
      .apply(
        "Rough Sample",
        Sample.any(OVERSAMPLING_FACTOR * dictionaryBuilder.preferredSampleSize)
      )
      .apply("Uniform Sample", Sample.fixedSizeGlobally(dictionaryBuilder.preferredSampleSize))
      .map("Build Dictionary from Samples") { dictionaryBuilder.buildDictionary(it) }
  }
}
