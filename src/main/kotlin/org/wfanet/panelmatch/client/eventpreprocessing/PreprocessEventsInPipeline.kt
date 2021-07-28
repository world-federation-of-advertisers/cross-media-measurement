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
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.wfanet.panelmatch.common.beam.parDo

/**
 * Runs preprocessing DoFns on input [PCollection] using specified ByteStrings as the crypto key and
 * pepper and outputs encrypted [PCollection]
 */
fun preprocessEventsInPipeline(
  events: PCollection<KV<ByteString, ByteString>>,
  maxByteSize: Int,
  pepper: ByteString,
  cryptokey: ByteString
): PCollection<KV<Long, ByteString>> {
  return preprocessEventsInPipeline(
    events,
    maxByteSize,
    HardCodedPepperProvider(pepper),
    HardCodedCryptoKeyProvider(cryptokey)
  )
}

/**
 * Runs preprocessing DoFns on input [PCollection] using specified SerializableFunctions to supply
 * cryptokey and pepper and outputs encrypted [PCollection]
 */
fun preprocessEventsInPipeline(
  events: PCollection<KV<ByteString, ByteString>>,
  maxByteSize: Int,
  pepperProvider: SerializableFunction<Void?, ByteString>,
  cryptoKeyProvider: SerializableFunction<Void?, ByteString>
): PCollection<KV<Long, ByteString>> {
  return events
    .parDo(BatchingDoFn(maxByteSize, EventSize))
    .parDo(EncryptionEventsDoFn(EncryptEvents(), pepperProvider, cryptoKeyProvider))
}
