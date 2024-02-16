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

package org.wfanet.panelmatch.client.exchangetasks

import com.google.protobuf.ByteString
import org.apache.beam.sdk.values.PCollection
import org.wfanet.panelmatch.client.eventpreprocessing.DeterministicCommutativeCipherKeyProvider
import org.wfanet.panelmatch.client.eventpreprocessing.EventPreprocessor
import org.wfanet.panelmatch.client.eventpreprocessing.HkdfPepperProvider
import org.wfanet.panelmatch.client.eventpreprocessing.IdentifierHashPepperProvider
import org.wfanet.panelmatch.client.eventpreprocessing.UnprocessedEvent
import org.wfanet.panelmatch.client.eventpreprocessing.preprocessEvents
import org.wfanet.panelmatch.client.eventpreprocessing.unprocessedEvent
import org.wfanet.panelmatch.client.privatemembership.DatabaseEntry
import org.wfanet.panelmatch.common.beam.map
import org.wfanet.panelmatch.common.beam.toSingletonView
import org.wfanet.panelmatch.common.compression.CompressionParameters

/** Preprocesses a database to be used in a private membership query. */
suspend fun ApacheBeamContext.preprocessEvents(
  eventPreprocessor: EventPreprocessor,
  deterministicCommutativeCipherKeyProvider:
    (ByteString) -> DeterministicCommutativeCipherKeyProvider,
  hkdfPepperProvider: (ByteString) -> HkdfPepperProvider,
  identifierPepperProvider: (ByteString) -> IdentifierHashPepperProvider,
  maxByteSize: Long,
) {

  val unprocessedEventData: PCollection<UnprocessedEvent> =
    readShardedPCollection("unprocessed-event-data", unprocessedEvent {})

  val hkdfPepper = readBlob("hkdf-pepper")
  val identifierPepper = readBlob("identifier-pepper")
  val encryptionKey = readBlob("encryption-key")

  val compressionParameters =
    readBlobAsPCollection("compression-parameters")
      .map("Parse as CompressionParameters") { CompressionParameters.parseFrom(it) }
      .toSingletonView("compression-parameters-singleton-view")

  val preprocessedEvents: PCollection<DatabaseEntry> =
    preprocessEvents(
      unprocessedEventData,
      maxByteSize,
      identifierPepperProvider(identifierPepper),
      hkdfPepperProvider(hkdfPepper),
      deterministicCommutativeCipherKeyProvider(encryptionKey),
      eventPreprocessor,
      compressionParameters,
    )

  preprocessedEvents.writeShardedFiles("preprocessed-event-data")
}
