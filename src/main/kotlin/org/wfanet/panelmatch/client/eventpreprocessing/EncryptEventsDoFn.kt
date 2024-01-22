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

import com.google.common.base.Stopwatch
import com.google.protobuf.ByteString
import java.util.concurrent.TimeUnit
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollectionView
import org.wfanet.panelmatch.common.beam.kvOf
import org.wfanet.panelmatch.common.compression.CompressionParameters

/**
 * Encrypts each of a batch of pairs of ByteStrings.
 *
 * The outputs are suitable for use as database entries in the Private Membership protocol.
 */
class EncryptEventsDoFn(
  private val eventPreprocessor: EventPreprocessor,
  private val identifierHashPepperProvider: IdentifierHashPepperProvider,
  private val hkdfPepperProvider: HkdfPepperProvider,
  private val deterministicCommutativeCipherKeyProvider: DeterministicCommutativeCipherKeyProvider,
  private val compressionParametersView: PCollectionView<CompressionParameters>,
) : DoFn<MutableList<KV<ByteString, ByteString>>, KV<Long, ByteString>>() {
  private val jniCallTimeDistribution =
    Metrics.distribution(BatchingDoFn::class.java, "jni-call-time-micros")

  @ProcessElement
  fun process(c: ProcessContext) {
    val events: MutableList<KV<ByteString, ByteString>> = c.element()
    val request = preprocessEventsRequest {
      cryptoKey = deterministicCommutativeCipherKeyProvider.get()
      identifierHashPepper = identifierHashPepperProvider.get()
      hkdfPepper = hkdfPepperProvider.get()
      compressionParameters = c.sideInput(compressionParametersView)
      for (event in events) {
        unprocessedEvents += unprocessedEvent {
          id = event.key
          data = event.value
        }
      }
    }
    val stopWatch: Stopwatch = Stopwatch.createStarted()
    val response: PreprocessEventsResponse = eventPreprocessor.preprocess(request)
    stopWatch.stop()
    jniCallTimeDistribution.update(stopWatch.elapsed(TimeUnit.MICROSECONDS))

    for (processedEvent in response.processedEventsList) {
      c.output(kvOf(processedEvent.encryptedId, processedEvent.encryptedData))
    }
  }
}
