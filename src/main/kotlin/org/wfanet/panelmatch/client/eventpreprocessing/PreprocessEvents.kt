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
import org.wfanet.panelmatch.client.privatemembership.DatabaseEntry
import org.wfanet.panelmatch.client.privatemembership.databaseEntry
import org.wfanet.panelmatch.client.privatemembership.encryptedEntry
import org.wfanet.panelmatch.client.privatemembership.lookupKey
import org.wfanet.panelmatch.common.beam.groupByKey
import org.wfanet.panelmatch.common.beam.kvOf
import org.wfanet.panelmatch.common.beam.map
import org.wfanet.panelmatch.common.beam.mapValues
import org.wfanet.panelmatch.common.beam.parDo
import org.wfanet.panelmatch.common.compression.CompressionParameters

fun preprocessEvents(
  unprocessedEvents: PCollection<UnprocessedEvent>,
  maxByteSize: Long,
  identifierHashPepperProvider: IdentifierHashPepperProvider,
  hkdfPepperProvider: HkdfPepperProvider,
  cryptoKeyProvider: DeterministicCommutativeCipherKeyProvider,
  eventPreprocessor: EventPreprocessor,
  compressionParametersView: PCollectionView<CompressionParameters>,
): PCollection<DatabaseEntry> {
  return unprocessedEvents.apply(
    "Preprocess Events",
    PreprocessEventsTransform(
      maxByteSize = maxByteSize,
      identifierHashPepperProvider = identifierHashPepperProvider,
      hkdfPepperProvider = hkdfPepperProvider,
      cryptoKeyProvider = cryptoKeyProvider,
      eventPreprocessor = eventPreprocessor,
      compressionParametersView = compressionParametersView,
    ),
  )
}

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
class PreprocessEventsTransform(
  private val maxByteSize: Long,
  private val identifierHashPepperProvider: IdentifierHashPepperProvider,
  private val hkdfPepperProvider: HkdfPepperProvider,
  private val cryptoKeyProvider: DeterministicCommutativeCipherKeyProvider,
  private val eventPreprocessor: EventPreprocessor,
  private val compressionParametersView: PCollectionView<CompressionParameters>,
) : PTransform<PCollection<UnprocessedEvent>, PCollection<DatabaseEntry>>() {

  override fun expand(events: PCollection<UnprocessedEvent>): PCollection<DatabaseEntry> {
    return events
      .map<UnprocessedEvent, KV<ByteString, ByteString>>("Map to KV") { kvOf(it.id, it.data) }
      .groupByKey("Group Events")
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
              compressionParametersView,
            )
          )
          .withSideInputs(compressionParametersView),
      )
      .map("Map to DatabaseEntry") {
        databaseEntry {
          this.lookupKey = lookupKey { key = it.key }
          this.encryptedEntry = encryptedEntry { data = it.value }
        }
      }
  }
}
