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
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionView
import org.wfanet.panelmatch.client.combinedEvents
import org.wfanet.panelmatch.common.beam.groupByKey
import org.wfanet.panelmatch.common.beam.mapValues
import org.wfanet.panelmatch.common.beam.parDo
import org.wfanet.panelmatch.common.compression.CompressionParameters

/**
 * Preprocesses events for use in Private Membership.
 *
 * The basic steps are:
 * 1. Group events by key -- and combine into a CombinedEvents proto.
 * 2. Group into batches to minimize JNI overhead.
 * 3. Compress and encrypt each key-value pair.
 *
 * This is a [PTransform] so it can fit in easily with existing Apache Beam pipelines.
 */
class PreprocessEvents(
  private val maxByteSize: Int,
  private val identifierHashPepperProvider: IdentifierHashPepperProvider,
  private val hkdfPepperProvider: HkdfPepperProvider,
  private val cryptoKeyProvider: DeterministicCommutativeCipherKeyProvider,
  private val compressionParametersView: PCollectionView<CompressionParameters>,
  private val eventPreprocessor: EventPreprocessor
) : PTransform<PCollection<KV<ByteString, ByteString>>, PCollection<KV<Long, ByteString>>>() {

  override fun expand(
    events: PCollection<KV<ByteString, ByteString>>
  ): PCollection<KV<Long, ByteString>> {
    return events
      .groupByKey()
      .mapValues("Make CombinedEvents") { combinedEvents { serializedEvents += it }.toByteString() }
      .parDo(BatchingDoFn(maxByteSize, EventSize), name = "Batch by $maxByteSize bytes")
      .apply(
        "Encrypt Batches",
        ParDo.of(
            EncryptEventsDoFn(
              eventPreprocessor,
              identifierHashPepperProvider,
              hkdfPepperProvider,
              cryptoKeyProvider,
              compressionParametersView
            ),
          )
          .withSideInputs(compressionParametersView)
      )
  }
}
